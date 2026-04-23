
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* global context pointer for signal handlers */
static supervisor_ctx_t *g_ctx = NULL;

/* ------------------------------------------------------------------ */
/* Helpers                                                              */
/* ------------------------------------------------------------------ */

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ------------------------------------------------------------------ */
/* Bounded buffer                                                       */
/* ------------------------------------------------------------------ */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Logging consumer thread                                              */
/* ------------------------------------------------------------------ */

void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    char log_path[PATH_MAX];
    int fd;

    while (1) {
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0)
            break;

        snprintf(log_path, sizeof(log_path), "%s/%s.log",
                 LOG_DIR, item.container_id);

        fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("open log file");
            continue;
        }

        write(fd, item.data, item.length);
        close(fd);
    }

    return NULL;
}

/* ------------------------------------------------------------------ */
/* Producer thread (one per container)                                  */
/* ------------------------------------------------------------------ */

typedef struct {
    int fd;
    supervisor_ctx_t *ctx;
    char id[CONTAINER_ID_LEN];
} producer_arg_t;

static void *producer_fn(void *a)
{
    producer_arg_t *pa = (producer_arg_t *)a;
    log_item_t item;
    ssize_t n;

    while ((n = read(pa->fd, item.data, sizeof(item.data))) > 0) {
        item.length = (size_t)n;
        strncpy(item.container_id, pa->id, sizeof(item.container_id) - 1);
        item.container_id[sizeof(item.container_id) - 1] = '\0';
        bounded_buffer_push(&pa->ctx->log_buffer, &item);
    }

    close(pa->fd);
    free(pa);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Container child entrypoint (runs after clone())                      */
/* ------------------------------------------------------------------ */

int child_fn(void *arg)
{
    child_config_t *config = (child_config_t *)arg;

    if (chroot(config->rootfs) != 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount /proc");
        return 1;
    }

    if (dup2(config->log_write_fd, STDOUT_FILENO) < 0) {
        perror("dup2 stdout");
        return 1;
    }

    if (dup2(config->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2 stderr");
        return 1;
    }

    close(config->log_write_fd);

    if (config->nice_value != 0) {
        if (nice(config->nice_value) < 0)
            perror("nice");
    }

    execl(config->command, config->command, NULL);

    perror("execl");
    return 1;
}

/* ------------------------------------------------------------------ */
/* Monitor ioctl helpers                                                */
/* ------------------------------------------------------------------ */

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/* ------------------------------------------------------------------ */
/* Signal handlers                                                      */
/* ------------------------------------------------------------------ */

static void signal_handler(int sig)
{
    (void)sig;
    if (g_ctx)
        g_ctx->should_stop = 1;
}

static void sigchld_handler(int sig)
{
    (void)sig;
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (!g_ctx)
            continue;

        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *c = g_ctx->containers;
        while (c) {
            if (c->host_pid == pid) {
                if (WIFEXITED(status)) {
                    c->exit_code = WEXITSTATUS(status);
                    c->state = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    c->exit_signal = WTERMSIG(status);
                    c->state = CONTAINER_KILLED;
                }
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }
}

/* ------------------------------------------------------------------ */
/* Spawn a container                                                    */
/* ------------------------------------------------------------------ */

static int spawn_container(supervisor_ctx_t *ctx, const control_request_t *req)
{
    int pipefd[2];
    char *stack;
    pid_t pid;
    child_config_t *config;
    container_record_t *record;
    producer_arg_t *parg;
    pthread_t producer;

    if (pipe(pipefd) < 0) {
        perror("pipe");
        return -1;
    }

    config = calloc(1, sizeof(child_config_t));
    if (!config) {
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }

    strncpy(config->id,      req->container_id, sizeof(config->id) - 1);
    strncpy(config->rootfs,  req->rootfs,        sizeof(config->rootfs) - 1);
    strncpy(config->command, req->command,       sizeof(config->command) - 1);
    config->nice_value   = req->nice_value;
    config->log_write_fd = pipefd[1];

    stack = malloc(STACK_SIZE);
    if (!stack) {
        free(config);
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }

pid = clone(child_fn,
            stack + STACK_SIZE,
            CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS | SIGCHLD,
            config);

if (pid > 0) {
    sleep(1);  // IMPORTANT

    if (ctx->monitor_fd >= 0) {
        register_with_monitor(ctx->monitor_fd,
                              req->container_id,
                              pid,
                              40 * 1024 * 1024,
                              64 * 1024 * 1024);
    }
}
    /* parent closes the write end — only the child needs it */
    close(pipefd[1]);
    free(stack);
    free(config);

    if (pid < 0) {
        perror("clone");
        close(pipefd[0]);
        return -1;
    }

    /* add container metadata record */
    record = calloc(1, sizeof(container_record_t));
    if (!record) {
        close(pipefd[0]);
        return -1;
    }
    strncpy(record->id, req->container_id, sizeof(record->id) - 1);
    record->host_pid         = pid;
    record->started_at       = time(NULL);
    record->state            = CONTAINER_RUNNING;
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    snprintf(record->log_path, sizeof(record->log_path),
             "%s/%s.log", LOG_DIR, req->container_id);

    pthread_mutex_lock(&ctx->metadata_lock);
    record->next  = ctx->containers;
    ctx->containers = record;
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* register with kernel monitor if available */
    if (ctx->monitor_fd >= 0) {
        register_with_monitor(ctx->monitor_fd, req->container_id,
                              pid,
                              req->soft_limit_bytes,
                              req->hard_limit_bytes);
    }

    /* spawn producer thread to forward container output to log buffer */
    parg = malloc(sizeof(producer_arg_t));
    if (!parg) {
        close(pipefd[0]);
        return -1;
    }
    parg->fd  = pipefd[0];
    parg->ctx = ctx;
    strncpy(parg->id, req->container_id, sizeof(parg->id) - 1);
    parg->id[sizeof(parg->id) - 1] = '\0';

    pthread_create(&producer, NULL, producer_fn, parg);
    pthread_detach(producer);

    return 0;
}

/* ------------------------------------------------------------------ */
/* Supervisor command handlers                                          */
/* ------------------------------------------------------------------ */

static void handle_ps(supervisor_ctx_t *ctx, int client_fd)
{
    control_response_t resp;
    char buf[2048] = {0};
    int offset = 0;

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *c = ctx->containers;
    while (c) {
        offset += snprintf(buf + offset, sizeof(buf) - offset,
                           "%-16s  pid=%-6d  state=%-10s  soft=%luMiB  hard=%luMiB\n",
                           c->id, c->host_pid,
                           state_to_string(c->state),
                           c->soft_limit_bytes >> 20,
                           c->hard_limit_bytes >> 20);
        c = c->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (offset == 0)
        snprintf(buf, sizeof(buf), "No containers.\n");

    memset(&resp, 0, sizeof(resp));
    resp.status = 0;
    snprintf(resp.message, sizeof(resp.message), "%s", buf);
    write(client_fd, &resp, sizeof(resp));
}

static void handle_stop(supervisor_ctx_t *ctx,
                        const control_request_t *req,
                        int client_fd)
{
    control_response_t resp;
    int found = 0;

    memset(&resp, 0, sizeof(resp));

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *c = ctx->containers;
    while (c) {
        if (strncmp(c->id, req->container_id, CONTAINER_ID_LEN) == 0) {
            found = 1;
            if (c->state == CONTAINER_RUNNING) {
                kill(c->host_pid, SIGTERM);
                c->state = CONTAINER_STOPPED;

                if (ctx->monitor_fd >= 0)
                    unregister_from_monitor(ctx->monitor_fd, c->id, c->host_pid);

                resp.status = 0;
                snprintf(resp.message, sizeof(resp.message),
                         "Stopped %s (pid %d)", c->id, c->host_pid);
            } else {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message),
                         "Container %s is not running (state: %s)",
                         c->id, state_to_string(c->state));
            }
            break;
        }
        c = c->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (!found) {
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message),
                 "Container %s not found", req->container_id);
    }

    write(client_fd, &resp, sizeof(resp));
}

static void handle_logs(supervisor_ctx_t *ctx,
                        const control_request_t *req,
                        int client_fd)
{
    control_response_t resp;
    char log_path[PATH_MAX];
    char line[256];
    FILE *f;

    (void)ctx;
    memset(&resp, 0, sizeof(resp));

    snprintf(log_path, sizeof(log_path), "%s/%s.log",
             LOG_DIR, req->container_id);

    f = fopen(log_path, "r");
    if (!f) {
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message),
                 "No logs found for container: %s", req->container_id);
        write(client_fd, &resp, sizeof(resp));
        return;
    }

    resp.status = 0;
    snprintf(resp.message, sizeof(resp.message),
             "=== Logs for %s ===", req->container_id);
    write(client_fd, &resp, sizeof(resp));

    while (fgets(line, sizeof(line), f))
        write(client_fd, line, strlen(line));

    fclose(f);
}

/* ------------------------------------------------------------------ */
/* Supervisor main loop                                                 */
/* ------------------------------------------------------------------ */

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sockaddr_un addr;
    struct sigaction sa;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;
    g_ctx = &ctx;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* create logs directory */
    mkdir(LOG_DIR, 0755);

    /* open kernel monitor device (optional — continue if absent) */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    printf("monitor_fd = %d\n", ctx.monitor_fd);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "Warning: /dev/container_monitor not found — "
                        "memory limits disabled\n");

    /* create UNIX domain socket for CLI ↔ supervisor IPC */
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        return 1;
    }

    unlink(CONTROL_PATH);
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }
    if (listen(ctx.server_fd, 8) < 0) {
        perror("listen");
        return 1;
    }

    /* install signal handlers */
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    sa.sa_handler = sigchld_handler;
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    /* start logging consumer thread */
    pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);

    fprintf(stderr, "Supervisor started. rootfs=%s  socket=%s\n",
            rootfs, CONTROL_PATH);

    /* event loop — accept and handle one CLI request at a time */
    while (!ctx.should_stop) {
        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR)
                continue;
            break;
        }

        control_request_t req;
        if (read(client_fd, &req, sizeof(req)) != (ssize_t)sizeof(req)) {
            close(client_fd);
            continue;
        }

        control_response_t resp;
        memset(&resp, 0, sizeof(resp));

        switch (req.kind) {
        case CMD_START:
        case CMD_RUN:
            if (spawn_container(&ctx, &req) == 0) {
                resp.status = 0;
                snprintf(resp.message, sizeof(resp.message),
                         "Started container %s", req.container_id);
            } else {
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message),
                         "Failed to start container %s", req.container_id);
            }
            write(client_fd, &resp, sizeof(resp));
            break;

        case CMD_PS:
            handle_ps(&ctx, client_fd);
            break;

        case CMD_STOP:
            handle_stop(&ctx, &req, client_fd);
            break;

        case CMD_LOGS:
            handle_logs(&ctx, &req, client_fd);
            break;

        default:
            resp.status = 1;
            snprintf(resp.message, sizeof(resp.message), "Unknown command");
            write(client_fd, &resp, sizeof(resp));
            break;
        }

        close(client_fd);
    }

    /* orderly shutdown */
    fprintf(stderr, "Supervisor shutting down...\n");
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);

    close(ctx.server_fd);
    unlink(CONTROL_PATH);

    /* free container records */
    container_record_t *c = ctx.containers;
    while (c) {
        container_record_t *next = c->next;
        free(c);
        c = next;
    }

    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

/* ------------------------------------------------------------------ */
/* CLI client — sends request to running supervisor                     */
/* ------------------------------------------------------------------ */

static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect: is supervisor running?");
        close(fd);
        return 1;
    }

    if (write(fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write request");
        close(fd);
        return 1;
    }

    if (read(fd, &resp, sizeof(resp)) != (ssize_t)sizeof(resp)) {
        perror("read response");
        close(fd);
        return 1;
    }

    printf("%s\n", resp.message);
    close(fd);
    return resp.status;
}

/* ------------------------------------------------------------------ */
/* CLI command builders                                                 */
/* ------------------------------------------------------------------ */

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> "
                "[--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> "
                "[--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}



int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}

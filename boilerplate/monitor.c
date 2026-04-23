#include <linux/timer.h>
#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME "container_monitor"
#define CHECK_INTERVAL_SEC 1

typedef struct monitor_node {
    pid_t pid;
    char container_id[32];
    unsigned long soft_limit;
    unsigned long hard_limit;
    int soft_warned;
    struct list_head list;
} monitor_node_t;

static LIST_HEAD(monitored_list);
static DEFINE_MUTEX(monitor_lock);

static struct timer_list monitor_timer;
static dev_t dev_num;
static struct cdev c_dev;
static struct class *cl;

/* RSS helper */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) {
        rss_pages = get_mm_rss(mm);
        mmput(mm);
    }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}

static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* Timer */
static void timer_callback(struct timer_list *t)
{
    monitor_node_t *node, *tmp;

    mutex_lock(&monitor_lock);

    list_for_each_entry_safe(node, tmp, &monitored_list, list) {

        long rss = get_rss_bytes(node->pid);

        if (rss < 0) {
            list_del(&node->list);
            kfree(node);
            continue;
        }

        if (!node->soft_warned && rss > node->soft_limit) {
            log_soft_limit_event(node->container_id, node->pid,
                                 node->soft_limit, rss);
            node->soft_warned = 1;
        }

        if (rss > node->hard_limit) {
            kill_process(node->container_id, node->pid,
                         node->hard_limit, rss);
            list_del(&node->list);
            kfree(node);
        }
    }

    mutex_unlock(&monitor_lock);

    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
}

/* IOCTL */
static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;
    monitor_node_t *node, *tmp;

    if (copy_from_user(&req, (void __user *)arg, sizeof(req)))
        return -EFAULT;

    if (cmd == MONITOR_REGISTER) {

        node = kmalloc(sizeof(*node), GFP_KERNEL);
        if (!node)
            return -ENOMEM;

        node->pid = req.pid;
        strncpy(node->container_id, req.container_id, sizeof(node->container_id)-1);
        node->soft_limit = req.soft_limit_bytes;
        node->hard_limit = req.hard_limit_bytes;
        node->soft_warned = 0;

        mutex_lock(&monitor_lock);
        list_add(&node->list, &monitored_list);
        mutex_unlock(&monitor_lock);

        return 0;
    }

    if (cmd == MONITOR_UNREGISTER) {

        mutex_lock(&monitor_lock);

        list_for_each_entry_safe(node, tmp, &monitored_list, list) {
            if (node->pid == req.pid) {
                list_del(&node->list);
                kfree(node);
                mutex_unlock(&monitor_lock);
                return 0;
            }
        }

        mutex_unlock(&monitor_lock);
        return -ENOENT;
    }

    return -EINVAL;
}

/* file ops */
static struct file_operations fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* init */
static int __init monitor_init(void)
{
    alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME);

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif

    device_create(cl, NULL, dev_num, NULL, DEVICE_NAME);

    cdev_init(&c_dev, &fops);
    cdev_add(&c_dev, dev_num, 1);

    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO "[container_monitor] Loaded\n");
    return 0;
}

/* exit */
static void __exit monitor_exit(void)
{
    del_timer_sync(&monitor_timer);

    monitor_node_t *node, *tmp;

    mutex_lock(&monitor_lock);

    list_for_each_entry_safe(node, tmp, &monitored_list, list) {
        list_del(&node->list);
        kfree(node);
    }

    mutex_unlock(&monitor_lock);

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Unloaded\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Container Memory Monitor");

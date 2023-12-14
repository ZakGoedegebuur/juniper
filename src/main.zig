const std = @import("std");

const Task = struct {
    func: *const fn () void,

    pub fn init(func: *const fn () void) Task {
        return Task {
            .func = func
        };
    }

    pub fn run(self: *Task) void {
        self.func();
    }
};

const ThreadContainer = struct {
    thread: std.Thread,
    tasks: std.DoublyLinkedList(Task),
    tasks_mtx: std.Thread.Mutex,
};

const ThreadPool = struct {
    const ShutdownPolicyEnum = enum { Continue, IfTasksFinished, IgnoreRemainingTasks };
    allocator: std.mem.Allocator,
    threads: std.ArrayList(ThreadContainer),
    //tasks: std.DoublyLinkedList(Task),
    //task_list_mtx: std.Thread.Mutex,

    shutdown_policy: ShutdownPolicyEnum = .Continue,

    pub fn init(allocator: std.mem.Allocator) ThreadPool {
        return ThreadPool {
            .allocator = allocator,
            .threads = std.ArrayList(ThreadContainer).init(allocator),
            //.tasks = std.DoublyLinkedList(Task){},
            //.task_list_mtx = std.Thread.Mutex{},
        };
    }

    const ThreadPoolConfig = struct {
        num_threads: ?u64 = null,
    };
    pub fn startThreads(self: *ThreadPool, config: ThreadPoolConfig) !void {
        try self.threads.resize(config.num_threads orelse try std.Thread.getCpuCount());

        std.debug.print("Num threads: {d}\n", .{self.threads.items.len});
        for (self.threads.items) |*thread_cont| {
            thread_cont.tasks = std.DoublyLinkedList(Task){};
            thread_cont.tasks_mtx = std.Thread.Mutex{};
            thread_cont.thread = try std.Thread.spawn(.{}, threadLoopFn, .{self, thread_cont});

            //thread.* = try std.Thread.spawn(.{}, threadLoopFn, .{self});
            //std.time.sleep(10 * std.time.ns_per_ms);
        }
    }

    const TaskCompleteEnum = enum { FinishAllTasks, FinishCurrentTask, CeaseImmediately };
    const ThreadPoolDeinitConfig = struct {
        finish_policy: TaskCompleteEnum = .FinishAllTasks,
    };
    pub fn deinit(self: *ThreadPool, config: ThreadPoolDeinitConfig) void {
        switch (config.finish_policy) {
            .FinishAllTasks => {
                self.shutdown_policy = .IfTasksFinished;
                for (self.threads.items) |*thread_cont| {
                    thread_cont.thread.join();
                }
            },
            .FinishCurrentTask => {
                self.shutdown_policy = .IgnoreRemainingTasks;

                for (self.threads.items) |*thread_cont| {
                    thread_cont.tasks_mtx.lock();
                    while (thread_cont.tasks.pop()) |n| {
                        self.allocator.destroy(n);
                    }
                    thread_cont.tasks_mtx.unlock();
                }

                for (self.threads.items) |*thread_cont| {
                    thread_cont.thread.join();
                }
            },
            .CeaseImmediately => {
                self.shutdown_policy = .IgnoreRemainingTasks;
                
                for (self.threads.items) |*thread_cont| {
                    thread_cont.tasks_mtx.lock();
                    while (thread_cont.tasks.pop()) |n| {
                        self.allocator.destroy(n);
                    }
                    thread_cont.tasks_mtx.unlock();
                }
                
                for (self.threads.items) |*thread_cont| {
                    thread_cont.thread.detach();
                }
            }
        }

        self.threads.clearAndFree();
    }

    pub fn scheduleTask(self: *ThreadPool, task: Task) !void {
        var least_busy_index: usize = 0;
        var least_busy_tcount: usize = 0;
        least_busy_tcount = @subWithOverflow(least_busy_tcount, 1)[0];
        for (self.threads.items, 0..) |*thread_cont, i| {
            if (thread_cont.tasks.len >= least_busy_tcount) least_busy_index = i;
        }

        var node = try self.allocator.create(std.DoublyLinkedList(Task).Node);
        node.* = std.DoublyLinkedList(Task).Node{
            .data = task,
        };

        self.threads.items[least_busy_index].tasks_mtx.lock();
        self.threads.items[least_busy_index].tasks.append(node);
        self.threads.items[least_busy_index].tasks_mtx.unlock();
    }

    fn threadLoopFn(thread_pool: *ThreadPool, thread_cont: *ThreadContainer) void {
        const threadID: u32 = std.Thread.getCurrentId();

        while (true) {
            switch (thread_pool.shutdown_policy) {
                .Continue => {},
                .IfTasksFinished => {
                    thread_cont.tasks_mtx.lock();
                    const remaining_tasks = thread_cont.tasks.len;
                    //std.debug.print("tasks remaining: {d}\n", .{remaining_tasks});
                    thread_cont.tasks_mtx.unlock();
                    if (remaining_tasks == 0) {
                        break;
                    }
                },
                .IgnoreRemainingTasks => break,
            }
            //std.debug.print("thread {} continuing\n", .{threadID});

            thread_cont.tasks_mtx.lock();
            var node_opt = thread_cont.tasks.popFirst();

            if (node_opt) |node| {
                // must make a copy of the task and destroy it before unlocking the mutex or if the thread is detached the memory may leak
                var owned_task = node.data; 
                thread_pool.allocator.destroy(node);
                thread_cont.tasks_mtx.unlock();

                owned_task.run();
            } else {
                thread_cont.tasks_mtx.unlock();

                //std.debug.print("thread {} out of available tasks!\n", .{threadID});
            }
        }
        
        std.debug.print("thread {} stopping\n", .{threadID});
    }
};

fn printTest() void {
    std.time.sleep(100 * std.time.ns_per_ms);
    std.debug.print("aaa\n", .{});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    var allocator = gpa.allocator();

    var tp = ThreadPool.init(allocator);
    try tp.startThreads(.{.num_threads = 2});
    for (0..5) |_| {
        try tp.scheduleTask(Task.init(&printTest));
    }

    std.time.sleep(2 * std.time.ns_per_s);
    tp.deinit(.{.finish_policy = .FinishAllTasks});
}
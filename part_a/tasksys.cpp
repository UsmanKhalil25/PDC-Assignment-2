#include "tasksys.h"
#include<atomic>
#include<thread>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads), num_threads(num_threads) {}
TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
    // No cleanup needed for this implementation
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks <= 0) {
        return;
    }

    std::atomic<int> next_task{0};
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, num_total_tasks]() {
            while (true) {
                int task_id = next_task.fetch_add(1, std::memory_order_relaxed);
                if (task_id >= num_total_tasks) {
                    break;
                }
                runnable->runTask(task_id, num_total_tasks);
            }
        });
    }

    for (auto& thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                               const std::vector<TaskID>& deps) {
    // Not implemented for this part
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // Not implemented for this part
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */


 const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
    this->current_runnable = nullptr;
    this->num_total_tasks = 0;
    this->next_task_id = 0;
    this->tasks_completed = 0;
    this->should_terminate = false;
    this->all_tasks_completed = true;
    
    // Create the worker threads
    for (int i = 0; i < num_threads; i++) {
        workers.push_back(std::thread(&TaskSystemParallelThreadPoolSpinning::worker_thread_func, this));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // Signal all threads to terminate
    {
        std::lock_guard<std::mutex> lock(mutex);
        should_terminate = true;
    }
    
    // Join all worker threads
    for (auto& thread : workers) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::worker_thread_func() {
    while (true) {
        // Check if we should terminate
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (should_terminate) {
                break;
            }
        }
        
        // Try to get a task
        int task_id = -1;
        IRunnable* runnable = nullptr;
        int total_tasks = 0;
        
        {
            std::lock_guard<std::mutex> lock(mutex);
            runnable = current_runnable;
            total_tasks = num_total_tasks;
            if (runnable != nullptr && next_task_id < total_tasks) {
                task_id = next_task_id++;
            }
        }
        
        // If we got a valid task, execute it
        if (task_id >= 0 && task_id < total_tasks && runnable != nullptr) {
            runnable->runTask(task_id, total_tasks);
            
            // Mark a task as completed
            {
                std::lock_guard<std::mutex> lock(mutex);
                tasks_completed++;
                
                // If all tasks are completed, notify the main thread
                if (tasks_completed == total_tasks) {
                    std::lock_guard<std::mutex> completion_lock(completion_mutex);
                    all_tasks_completed = true;
                    completion_cv.notify_one();
                }
            }
        }
        
        // If there are no tasks or we've processed all tasks, yield
        std::this_thread::yield();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks == 0) {
        return;  // Nothing to do
    }
    
    // Reset task system state for a new bulk task launch
    {
        std::lock_guard<std::mutex> lock(mutex);
        next_task_id = 0;
        tasks_completed = 0;
        current_runnable = runnable;
        this->num_total_tasks = num_total_tasks;
    }
    
    {
        std::lock_guard<std::mutex> lock(completion_mutex);
        all_tasks_completed = false;
    }
    
    // Wait for all tasks to complete
    {
        std::unique_lock<std::mutex> lock(completion_mutex);
        completion_cv.wait(lock, [this] { return all_tasks_completed; });
    }
    
    // Clear the current runnable
    {
        std::lock_guard<std::mutex> lock(mutex);
        current_runnable = nullptr;
        this->num_total_tasks = 0;
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // Not implemented for this part
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // Not implemented for this part
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

 const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads), num_threads(num_threads),
      current_runnable(nullptr), total_tasks(0), next_task(0), tasks_completed(0),
      running(false), shutdown(false)
{
    for (int i = 0; i < num_threads; ++i) {
        workers.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::worker_thread, this));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::lock_guard<std::mutex> lock(mtx);
        shutdown = true;
    }
    cv_task.notify_all();
    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::worker_thread() {
    while (true) {
        int task_id = -1;
        IRunnable* runnable = nullptr;
        int total = 0;

        // Acquire a task
        {
            std::unique_lock<std::mutex> lock(mtx);
            // Wait until there is work to do or the pool is shutting down.
            cv_task.wait(lock, [this] { return shutdown || (running && next_task < total_tasks); });
            if (shutdown) {
                break;
            }
            // There is work: grab a task index.
            if (next_task < total_tasks) {
                task_id = next_task++;
                runnable = current_runnable;
                total = total_tasks;
            }
        }

        // If we got a valid task, execute it outside the lock.
        if (task_id >= 0 && runnable != nullptr) {
            runnable->runTask(task_id, total);

            // Update the task completion count.
            std::unique_lock<std::mutex> lock(mtx);
            tasks_completed++;
            if (tasks_completed == total_tasks) {
                // All tasks are complete: notify the waiting main thread.
                running = false;  // mark the batch as finished
                cv_completion.notify_one();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks <= 0) {
        return;
    }
    
    {
        std::unique_lock<std::mutex> lock(mtx);
        // Set up the new batch of tasks.
        current_runnable = runnable;
        total_tasks = num_total_tasks;
        next_task = 0;
        tasks_completed = 0;
        running = true;
    }
    // Wake up all workers to start processing the new tasks.
    cv_task.notify_all();

    // Wait until all tasks are completed.
    {
        std::unique_lock<std::mutex> lock(mtx);
        cv_completion.wait(lock, [this] { return tasks_completed == total_tasks; });
        // Reset state (optional, current_runnable can be set to nullptr)
        current_runnable = nullptr;
        total_tasks = 0;
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                               const std::vector<TaskID>& deps) {
    // Not implemented for this part.
    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    // Not implemented for this part.
    return;
}
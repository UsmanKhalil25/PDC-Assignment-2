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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    this->num_threads = num_threads;
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks) {
	std::atomic<int> task_id(0);
	std::thread threads[this->num_threads];

	for (auto &thread: threads) {
		thread = std::thread([&task_id, num_total_tasks, runnable] {
			for (int id = task_id++; id < num_total_tasks; id = task_id++)
				runnable->runTask(id, num_total_tasks);
		});
	}

	for (auto &thread: threads) { 
        thread.join(); 
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads) {
	exit_flag = false;
	for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(&TaskSystemParallelThreadPoolSpinning::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
	exit_flag = true;
	for (auto &thread: threads) { 
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::workerLoop() {
	int taskId;
	while (!exit_flag) {
		taskId = -1;
		queue_mutex.lock();
		if (!task_queue.empty()) {
			taskId = task_queue.front();
			task_queue.pop();
		}
		queue_mutex.unlock();

		if (taskId != -1) {
			runnables->runTask(taskId, num_of_total_tasks);
			task_remaining--;
		}
	}
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks) {
	runnables = runnable;
	task_remaining = num_total_tasks;
	num_of_total_tasks = num_total_tasks;
	for (int i = 0; i < num_total_tasks; i++) {
		queue_mutex.lock();
		task_queue.push(i);
		queue_mutex.unlock();
	}
	while (task_remaining);
}


TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    exit_flag = false;
    for (int i = 0; i < num_threads; ++i) {
      threads.emplace_back( & TaskSystemParallelThreadPoolSleeping::workerLoop, this);
    }
}
  
TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    exit_flag = true;
    queue_cond.notify_all();
    for (auto & thread: threads) {
      thread.join();
    }
}
  
void TaskSystemParallelThreadPoolSleeping::workerLoop() {
    int taskId;
    while (true) {
      while (true) {
        std::unique_lock < std::mutex > lock(queue_mutex);
        queue_cond.wait(lock, [] {
          return true;
        });
        if (exit_flag) return;
        if (task_queue.empty()) continue;
        taskId = task_queue.front();
        task_queue.pop();
        break;
      }
      runnables -> runTask(taskId, num_of_total_tasks);
      std::unique_lock < std::mutex > lock(counter_lock);
      task_remaining--;
      if (task_remaining) queue_cond.notify_all();
      else counter_cond.notify_one();
    }
}
  
void TaskSystemParallelThreadPoolSleeping::run(IRunnable * runnable, int num_total_tasks) {
    runnables = runnable;
    task_remaining = num_total_tasks;
    num_of_total_tasks = num_total_tasks;
    queue_mutex.lock();
    for (int i = 0; i < num_total_tasks; i++) {
      task_queue.push(i);
    }
    queue_mutex.unlock();
    queue_cond.notify_all();
    while (true) {
      std::unique_lock < std::mutex > lock(counter_lock);
      counter_cond.wait(lock, [] {
        return true;
      });
      if (!task_remaining) return;
    }
}


TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}

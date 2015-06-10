A simple tool that simulates queue processing based on producer-consumer model
with limited resources.

To run:

```
python ./pq.py
```


You can configure the parameters in the script itself such as:

  * `q_size`            : size of the queue
  * `max_machines`      : number of machine that can process a task
  * `max_task_time`     : maximum task processing time in seconds (will be
                          randomized up to this number)
  * `num_tasks_per_sec` : maximum number of tasks to dispatch per second (will
                          be randomized up to this number)
  * `dispatch_interval` : Interval period of how often tasks should be dispatched

import queue
from threading import Thread

class TaskQueue(queue.Queue):
    """
    A class to run tasks in parallel

    Example:
        q = TaskQueue(num_workers=2)

        q.add_task(task, a, b)

        q.join() # blocking
    """

    def __init__(self, num_workers=1):
        queue.Queue.__init__(self)
        self.num_workers = num_workers
        self.start_workers()

    def add_task(self, task, *args, **kwargs):
        """Add task to the queue

        Args:
            task (func): Function to be run
        """
        args = args or ()
        kwargs = kwargs or {}
        self.put((task, args, kwargs))

    def start_workers(self):
        for i in range(self.num_workers):
            t = Thread(target=self.worker)
            t.daemon = True
            t.start()

    def worker(self):
        while True:
            item, args, kwargs = self.get()
            item(*args, **kwargs)  
            self.task_done()
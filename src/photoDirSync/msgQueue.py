
import queue

class Q:
    name: str
    q: queue.Queue
    def __init__(self, name: str):
        self.name = name
        self.q = queue.Queue()

    def put(self, label: str, msg:str):
        self.q.put( item=(label,msg), block=False)

    def get_nowait(self):
        return self.q.get_nowait()

    def qsize(self):
        return self.q.qsize()
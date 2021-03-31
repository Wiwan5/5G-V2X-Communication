from tkinter import *
from threading import Thread

from .accident import Accident


class initInterface(Thread):
    def __init__(self, transaction,topic):
        Thread.__init__(self)
        self.root = Tk()
        self.root.title(topic)
        self.frame1 = Frame()
        self.frame1.pack(side=LEFT, fill=Y)
        self.frame1.config(bg="skyblue")
        self.accident = Accident(self.frame1, transaction,topic)

    def run(self):
        self.root.mainloop()

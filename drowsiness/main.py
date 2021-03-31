from tkinter import *
from threading import Thread

from .drowsiness import Drowsiness


class initInterfaceDDS(Thread):
    def __init__(self, transaction):
        Thread.__init__(self)
        self.root = Tk()
        self.root.title("DDS")
        self.frame1 = Frame()
        self.frame1.pack(side=LEFT, fill=Y)
        self.frame1.config(bg="skyblue")
        self.drowsiness = Drowsiness(self.frame1, transaction)

    def run(self):
        self.root.mainloop()



from tkinter import *


class Accident:
    def __init__(self, root_frame, transaction):
        self.root_frame = root_frame
        self.transaction = transaction
        self.home_head = Frame(root_frame)
        self.home_head.config(bg="skyblue")
        self.home_head.pack()

        self.header = Label(self.home_head,
                            text="Accident",
                            bg="skyblue",
                            font=("Helvetica", 14))
        self.header.grid(row=0, column=0, padx=110, pady=5)

        self.new_message_btn = Button(self.home_head,
                                      text='Alert!',
                                      font=("Helvetica", 12),
                                      bg="#FFBA31",
                                      command=self.transaction.create_transaction_accident)
        self.new_message_btn.grid(row=0, column=1)

        
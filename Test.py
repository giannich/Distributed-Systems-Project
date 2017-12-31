from Node import Node

n1 = Node(0, "Gianni", 13820, "224.138.138.138", 12345)
n3 = Node(1, "Jordan", 13822, "224.138.138.139", 12345)

n2 = Node(0, "xxxxxx", 13821, "224.138.138.138", 12345)

n1.register()
n1.process()
n3.register()
n3.process()
n2.register()
n2.process()
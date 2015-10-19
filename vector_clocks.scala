/*
VECTOR CLOCKS

assumptions:
- Whenever a process does work, increment the logical clock value of the node in the vector
- Whenever a process sends a message, include the full vector of logical clocks
- When a message is received:
  * update each element in the vector to be max(local, received)
  * increment the logical clock value representing the current node in the vector
*/

case class NodeId(id: String)
case class Node(id: NodeId, vc: VectorClock)

object Node {
  def apply(id: NodeId) = new Node(id, VectorClock(Map(id -> 0)))

  def !(sender: Node, receiver: Node): (Node, Node) = {
    val senderNode = sender.copy(vc = VectorClock.increment(sender.id, sender.vc))
    val receiverNode = receiver.copy(vc = VectorClock.merge(receiver.id, senderNode.vc, receiver.vc))

    (senderNode, receiverNode)
  }
}

case class VectorClock(values: Map[NodeId, Int])

object VectorClock {

  def increment(id: NodeId, vc: VectorClock): VectorClock = {
    val counter = vc.values(id) + 1
    val value = (id -> counter)
    VectorClock(vc.values + value)
  }

  def merge(receiverId: NodeId, vc1: VectorClock, vc2: VectorClock): VectorClock = {

    val mergedValues = vc1.values ++ vc2.values

    val mergedCounter = (vc1.values.get(receiverId), vc2.values.get(receiverId)) match {
      case (Some(counter1), Some(counter2)) => scala.math.max(counter1, counter2)
      case (None, Some(counter2))           => counter2
      case (Some(counter1), None)           => counter1
      case (None, None)                     => 0
    }

    val counter = mergedCounter + 1

    VectorClock(mergedValues + (receiverId -> counter))
  }

}

// RUNTIME

val node1 = Node(NodeId("A"))
val node2 = Node(NodeId("B"))
val node3 = Node(NodeId("C"))


val (n1_1, n2_1) = Node !(sender = node1, receiver = node2)
assert(n1_1.vc == VectorClock(Map(NodeId("A") -> 1)))
assert(n2_1.vc == VectorClock(Map(NodeId("A") -> 1, NodeId("B") -> 1)))

val (n2_2, n3_1) = Node !(sender = n2_1, receiver = node3)
assert(n2_2.vc == VectorClock(Map(NodeId("A") -> 1, NodeId("B") -> 2)))
assert(n3_1.vc == VectorClock(Map(NodeId("A") -> 1, NodeId("B") -> 2, NodeId("C") -> 1)))



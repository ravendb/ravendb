using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron;
using Voron.Data.Compact;
using Voron.Tests;
using Xunit;

namespace FastTests.Voron.Compact
{
    public unsafe class PrefixTreeStorageTests : StorageTest
    {
        public sealed class SampleData : IEquatable<SampleData>
        {
            public string Data;

            bool IEquatable<SampleData>.Equals(SampleData other)
            {
                return other.Data == this.Data;
            }
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);
        }


        public static void DumpKeys(PrefixTree tree)
        {
            Console.WriteLine("Tree stored order");

            var currentPtr = tree.State.Head.NextPtr;

            while (currentPtr != PrefixTree.Constants.InvalidNodeName && currentPtr != PrefixTree.Constants.TombstoneNodeName)
            {
                PrefixTree.Leaf* node = (PrefixTree.Leaf*)tree.ReadNodeByName(currentPtr);
                Debug.Assert(node->IsLeaf);

                var key = tree.ReadKey(node->DataPtr);
                Console.WriteLine(key.ToString());

                currentPtr = node->NextPtr;
            }
        }

        public static void DumpTree(PrefixTree tree)
        {
            if (tree.Count == 0)
            {
                Console.WriteLine("Tree is empty.");
            }
            else
            {
                DumpNodes(tree, tree.Root, null, 0, 0);
            }
        }

        private static int DumpNodes(PrefixTree tree, PrefixTree.Node* node, PrefixTree.Node* parent, int nameLength, int depth)
        {
            if (node == null)
                return 0;

            for (int i = depth; i-- != 0;)
                Console.Write('\t');

            if (node->IsInternal)
            {
                var internalNode = (PrefixTree.Internal*)node;

                var jumpLeft = tree.ReadNodeByName(internalNode->JumpLeftPtr);
                var jumpRight = tree.ReadNodeByName(internalNode->JumpRightPtr);

                Console.WriteLine(string.Format("Node {0} (name length: {1}) Jump left: {2} Jump right: {3}", tree.ToDebugString(node), nameLength, tree.ToDebugString(jumpLeft), tree.ToDebugString(jumpRight)));

                var left = tree.ReadNodeByName(internalNode->LeftPtr);
                var right = tree.ReadNodeByName(internalNode->RightPtr);

                return 1 + DumpNodes(tree, left, node, internalNode->ExtentLength + 1, depth + 1)
                         + DumpNodes(tree, right, node, internalNode->ExtentLength + 1, depth + 1);
            }
            else
            {
                Console.WriteLine(string.Format("Node {0} (name length: {1})", tree.ToDebugString(node), nameLength));

                return 1;
            }
        }


        public static void StructuralVerify(PrefixTree tree)
        {
            // These are invariants.
            Assert.NotNull(tree.State);
            Assert.NotNull(tree.NodesTable);
            Assert.Equal(tree.State.Tail.NextPtr, PrefixTree.Constants.InvalidNodeName);
            Assert.Equal(tree.State.Head.PreviousPtr, PrefixTree.Constants.InvalidNodeName);

            // Either the root does not exist or the root is internal and have name length == 0
            Assert.True(tree.ReadNodeByName(PrefixTree.Constants.RootNodeName) == null || tree.Root->NameLength == 0);
            Assert.True(tree.State.Items == 0 && tree.NodesTable.Count == 0 || tree.Count == tree.NodesTable.Values.Count() + 1);

            if (tree.Count == 0)
            {
                Assert.Equal(tree.State.Head.NextPtr, PrefixTree.Constants.TailNodeName);
                Assert.Equal(tree.State.Tail.PreviousPtr, PrefixTree.Constants.HeadNodeName);

                Assert.Equal(0, tree.NodesTable.Count);

                return; // No more to check for an empty trie.
            }

            Assert.NotEqual(tree.State.Head.NextPtr, PrefixTree.Constants.InvalidNodeName);
            Assert.NotEqual(tree.State.Tail.PreviousPtr, PrefixTree.Constants.InvalidNodeName);

            // We check if the first leaf node is pointing back to a tombstone.
            var head = (PrefixTree.Leaf*)tree.ReadNodeByName(tree.State.Head.NextPtr);
            Assert.True(head->IsLeaf);
            Assert.Equal(head->PreviousPtr, PrefixTree.Constants.HeadNodeName);

            // We check if the last leaf node is pointing forward to a tombstone.
            var tail = (PrefixTree.Leaf*)tree.ReadNodeByName(tree.State.Tail.PreviousPtr);
            Assert.True(tail->IsLeaf);
            Assert.Equal(tail->NextPtr, PrefixTree.Constants.TailNodeName);

            var root = tree.Root;
            var nodes = new HashSet<long>();

            foreach (var nodePtr in tree.NodesTable.Values)
            {
                var node = (PrefixTree.Internal*)tree.ReadNodeByName(nodePtr);
                Assert.True(node->IsInternal);

                int handleLength = tree.GetHandleLength(node);

                Assert.True(root == node || tree.GetHandleLength(root) < handleLength); // All handled of lower nodes must be bigger than the root.

                var referenceNode = tree.ReadNodeByName(node->ReferencePtr);
                var backReferenceNode = tree.ReadNodeByName(referenceNode->ReferencePtr);

                Assert.True(node == backReferenceNode); // The reference of the reference should be itself.

                nodes.Add(nodePtr);
            }

            Assert.Equal(tree.NodesTable.Values.Count(), nodes.Count); // We are ensuring there are no repeated nodes in the hash table. 

            if (tree.Count == 1)
            {
                Assert.True(tree.Root == head);
                Assert.True(tree.Root == tail);
            }
            else
            {
                var toRight = head;
                var toLeft = tail;

                for (int i = 1; i < tree.Count; i++)
                {
                    // Ensure there is name order in the linked list of leaves.
                    var nextRight = (PrefixTree.Leaf*)tree.ReadNodeByName(toRight->NextPtr);
                    Assert.True(nextRight->IsLeaf);
                    Assert.True(tree.Name(toRight).CompareTo(tree.Name(nextRight)) <= 0);

                    var previousLeft = (PrefixTree.Leaf*)tree.ReadNodeByName(toLeft->PreviousPtr);
                    Assert.True(previousLeft->IsLeaf);                   
                    Assert.True(tree.Name(toLeft).CompareTo(tree.Name(previousLeft)) >= 0);

                    toRight = nextRight;
                    toLeft = previousLeft;
                }

                var leaves = new HashSet<long>();
                var references = new HashSet<long>();

                int numberOfNodes = VisitNodes(tree, tree.Root, null, 0, nodes, leaves, references);
                Assert.Equal(2 * tree.Count - 1, numberOfNodes); // The amount of nodes is directly correlated with the tree size.
                Assert.Equal(tree.Count, leaves.Count); // The size of the tree is equal to the amount of leaves in the tree.

                int counter = 0;
                foreach (var leafPtr in leaves)
                {
                    var leaf = (PrefixTree.Leaf*)tree.ReadNodeByName(leafPtr);
                    Assert.True(leaf->IsLeaf);

                    // We check the reference of the leaf has been accounted for as an internal node. 
                    if (references.Contains(leaf->ReferencePtr))
                        counter++;
                }

                Assert.Equal(tree.Count - 1, counter);
            }

            Assert.Equal(0, nodes.Count);

            tree.NodesTable.VerifyStructure();
        }

        private static int VisitNodes(PrefixTree tree, PrefixTree.Node* node,
                                     PrefixTree.Node* parent, int nameLength,
                                     HashSet<long> nodes,
                                     HashSet<long> leaves,
                                     HashSet<long> references)
        {
            if (node == null)
                return 0;

            Assert.True(nameLength <= tree.GetExtentLength(node));

            if (parent->IsInternal)
            {
                Assert.True(tree.Extent(parent).Equals(tree.Extent(node).SubVector(0, ((PrefixTree.Internal*)parent)->ExtentLength)));
            }

            if (node->IsInternal)
            {
                var leafNode = (PrefixTree.Leaf*)tree.ReadNodeByName(node->ReferencePtr);

                Assert.NotNull(leafNode->IsLeaf); // We ensure that internal node references are leaves. 

                Assert.True(references.Add(leafNode->DataPtr));
                Assert.True(nodes.Remove((long)node));

                var handle = tree.Handle(node);

                var allNodes = tree.NodesTable.Values.Select(x => tree.Handle(tree.ReadNodeByName(x)));

                Assert.True(allNodes.Contains(handle));

                var internalNode = (PrefixTree.Internal*)node;
                int jumpLength = tree.GetJumpLength(internalNode);

                var jumpLeft = tree.ReadNodeByName(internalNode->LeftPtr);
                while (jumpLeft->IsInternal && jumpLength > ((PrefixTree.Internal*)jumpLeft)->ExtentLength)
                    jumpLeft = tree.ReadNodeByName(((PrefixTree.Internal*)jumpLeft)->LeftPtr);

                Assert.Equal(internalNode->JumpLeftPtr, (long)jumpLeft);

                var jumpRight = tree.ReadNodeByName(internalNode->RightPtr);
                while (jumpRight->IsInternal && jumpLength > ((PrefixTree.Internal*)jumpRight)->ExtentLength)
                    jumpRight = tree.ReadNodeByName(((PrefixTree.Internal*)jumpRight)->RightPtr);

                Assert.Equal(internalNode->JumpRightPtr, (long)jumpRight);

                var left = tree.ReadNodeByName(internalNode->LeftPtr);
                var right = tree.ReadNodeByName(internalNode->RightPtr);

                return 1 + VisitNodes(tree, left, node, internalNode->ExtentLength + 1, nodes, leaves, references)
                         + VisitNodes(tree, right, node, internalNode->ExtentLength + 1, nodes, leaves, references);
            }
            else
            {
                Assert.True(node->IsLeaf);
                var leafNode = (PrefixTree.Leaf*)node;

                Assert.True(leaves.Add((long)leafNode)); // We haven't found this leaf somewhere else.
                Assert.Equal(tree.Name(node).Count, tree.GetExtentLength(node)); // This is a leaf, the extent is the key

                var reference = tree.ReadNodeByName(parent->ReferencePtr);
                Assert.True(reference->IsLeaf); // We ensure that internal node references are leaves. 

                return 1;
            }
        }
    }
}

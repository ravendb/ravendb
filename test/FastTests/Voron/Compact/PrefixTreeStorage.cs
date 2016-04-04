using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Voron;
using Voron.Data.Compact;
using Voron.Data.Tables;
using Voron.Util.Conversion;
using Xunit;

namespace FastTests.Voron.Compact
{
    public unsafe class PrefixTreeStorageTests : StorageTest
    {
        protected string Name = "docs";
        protected TableSchema DocsSchema;

        protected void InitializeStorage()
        {
            InitializeStorage(Name);
        }

        protected virtual void InitializeStorage(string treeName)
        {
            using (var tx = Env.WriteTransaction())
            {
                DocsSchema.Create(tx, treeName);

                tx.Commit();
            }
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);

            options.ManualFlushing = true;

            DocsSchema = new TableSchema()
                .DefineKey(new TableSchema.SchemaIndexDef
                {
                    StartIndex = 0,
                    Type = TableIndexType.Compact,
                });
        }

        public unsafe long SetHelper(Table table, params object[] args)
        {
            var builder = new TableValueBuilder();
            var buffers = new List<byte[]>();
            foreach (var o in args)
            {
                var s = o as string;
                if (s != null)
                {
                    buffers.Add(Encoding.UTF8.GetBytes(s));
                    continue;
                }

                var slice = o as Slice;
                if (slice != null )
                {
                    if (slice.Array == null)
                        throw new NotSupportedException();

                    buffers.Add(slice.Array);
                    continue;
                }

                var l = (long)o;
                buffers.Add(EndianBitConverter.Big.GetBytes(l));
            }

            var handles1 = new List<GCHandle>();
            foreach (var buffer in buffers)
            {
                var gcHandle1 = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                handles1.Add(gcHandle1);
                builder.Add((byte*)gcHandle1.AddrOfPinnedObject(), buffer.Length);
            }

            var handles = handles1;

            long id = table.Set(builder);

            foreach (var gcHandle in handles)
            {
                gcHandle.Free();
            }

            return id;
        }

        public static void DumpKeys(PrefixTree tree)
        {
            Console.WriteLine("Tree stored order");

            var currentPtr = tree.State.Head.NextPtr;

            while (currentPtr != PrefixTree.Constants.InvalidNodeName && currentPtr > PrefixTree.Constants.TombstoneNodeName)
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
                DumpNodes(tree, tree.State.RootNodeName, PrefixTree.Constants.InvalidNodeName, 0, 0);
            }
        }

        private static int DumpNodes(PrefixTree tree, long nodeName, long parentName, int nameLength, int depth)
        {
            if (nodeName == PrefixTree.Constants.InvalidNodeName)
                return 0;

            for (int i = depth; i-- != 0;)
                Console.Write('\t');

            PrefixTree.Node* node = tree.ReadNodeByName(nodeName);
            if (node->IsInternal)
            {
                var internalNode = (PrefixTree.Internal*)node;

                var jumpLeft = tree.ReadNodeByName(internalNode->JumpLeftPtr);
                var jumpRight = tree.ReadNodeByName(internalNode->JumpRightPtr);

                Console.WriteLine(string.Format("Node {0} (name length: {1}) Jump left: {2} Jump right: {3}", tree.ToDebugString(node), nameLength, tree.ToDebugString(jumpLeft), tree.ToDebugString(jumpRight)));

                return 1 + DumpNodes(tree, internalNode->LeftPtr, nodeName, internalNode->ExtentLength + 1, depth + 1)
                         + DumpNodes(tree, internalNode->RightPtr, nodeName, internalNode->ExtentLength + 1, depth + 1);
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
            Assert.True(tree.ReadNodeByName(tree.State.RootNodeName) == null || tree.Root->NameLength == 0);
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

                int numberOfNodes = VisitNodes(tree, tree.State.RootNodeName, PrefixTree.Constants.InvalidNodeName, 0, nodes, leaves, references);
                Assert.Equal(2 * tree.Count - 1, numberOfNodes); // The amount of nodes is directly correlated with the tree size.
                Assert.Equal(tree.Count, leaves.Count); // The size of the tree is equal to the amount of leaves in the tree.

                int counter = 0;
                foreach (var leafPtr in leaves)
                {
                    var leaf = (PrefixTree.Leaf*)tree.ReadNodeByName(leafPtr);
                    Assert.True(leaf->IsLeaf);

                    // We check the reference of the leaf has been accounted for as an internal node. 
                    if (references.Contains(leaf->DataPtr))
                        counter++;
                }

                Assert.Equal(tree.Count - 1, counter);
            }

            Assert.Equal(0, nodes.Count);

            tree.NodesTable.VerifyStructure();
        }

        private static int VisitNodes(PrefixTree tree, long nodeName,
                                     long parentName, int nameLength,
                                     HashSet<long> nodes,
                                     HashSet<long> leaves,
                                     HashSet<long> references)
        {
            if (nodeName == PrefixTree.Constants.InvalidNodeName)
                return 0;

            var node = tree.ReadNodeByName(nodeName);
            Assert.True(nameLength <= tree.GetExtentLength(node));

            PrefixTree.Node* parent = null;
            if (parentName != PrefixTree.Constants.InvalidNodeName)
            {
                parent = tree.ReadNodeByName(parentName);
                if ( parent->IsInternal)
                {
                    Assert.True(tree.Extent(parent).Equals(tree.Extent(node).SubVector(0, ((PrefixTree.Internal*)parent)->ExtentLength)));
                }                
            }

            if (node->IsInternal)
            {
                var leafNode = (PrefixTree.Leaf*)tree.ReadNodeByName(node->ReferencePtr);

                Assert.True(leafNode->IsLeaf); // We ensure that internal node references are leaves. 

                Assert.True(references.Add(leafNode->DataPtr));
                Assert.True(nodes.Remove(nodeName));

                var handle = tree.Handle(node);

                var allNodes = tree.NodesTable.Values.Select(x => tree.Handle(tree.ReadNodeByName(x)));

                Assert.True(allNodes.Contains(handle));

                var internalNode = (PrefixTree.Internal*)node;
                int jumpLength = tree.GetJumpLength(internalNode);
                Assert.NotEqual(PrefixTree.Constants.InvalidNodeName, internalNode->ReferencePtr);

                var jumpLeftName = internalNode->LeftPtr;
                var jumpLeft = tree.ReadNodeByName(jumpLeftName);
                while (jumpLeft->IsInternal && jumpLength > ((PrefixTree.Internal*)jumpLeft)->ExtentLength)
                {
                    jumpLeftName = ((PrefixTree.Internal*)jumpLeft)->LeftPtr;
                    jumpLeft = tree.ReadNodeByName(jumpLeftName);
                }                    

                Assert.Equal(internalNode->JumpLeftPtr, jumpLeftName);

                var jumpRightName = internalNode->RightPtr;
                var jumpRight = tree.ReadNodeByName(jumpRightName);
                while (jumpRight->IsInternal && jumpLength > ((PrefixTree.Internal*)jumpRight)->ExtentLength)
                {
                    jumpRightName = ((PrefixTree.Internal*)jumpRight)->RightPtr;
                    jumpRight = tree.ReadNodeByName(jumpRightName);
                }                    

                Assert.Equal(internalNode->JumpRightPtr, jumpRightName);

                var left = internalNode->LeftPtr;
                var right = internalNode->RightPtr;

                return 1 + VisitNodes(tree, left, nodeName, internalNode->ExtentLength + 1, nodes, leaves, references)
                         + VisitNodes(tree, right, nodeName, internalNode->ExtentLength + 1, nodes, leaves, references);
            }
            else
            {
                Assert.True(node->IsLeaf);
                var leafNode = (PrefixTree.Leaf*)node;

                Assert.True(leaves.Add(nodeName)); // We haven't found this leaf somewhere else.
                Assert.Equal(tree.Name(node).Count, tree.GetExtentLength(node)); // This is a leaf, the extent is the key

                var reference = tree.ReadNodeByName(parent->ReferencePtr);
                Assert.True(reference->IsLeaf); // We ensure that internal node references are leaves. 

                return 1;
            }
        }

        protected long AddAndDumpToPrefixTree(PrefixTree tree, Table table, string key, string value)
        {
            long res = AddToPrefixTree(tree, table, key, value);
            DumpTree(tree);
            return res;
        }

        protected long AddToPrefixTree(PrefixTree tree, Table table, string key, string value)
        {
            return AddToPrefixTree(tree, table, new Slice(Encoding.UTF8.GetBytes(key)), value);
        }

        protected long AddToPrefixTree(PrefixTree tree, Table table, Slice key, string value)
        {
            return SetHelper(table, key, value);
        }
    }
}

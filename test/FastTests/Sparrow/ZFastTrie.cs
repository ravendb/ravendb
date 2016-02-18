using Sparrow;
using Sparrow.Binary;
using Sparrow.Collections;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Extensions;

namespace FastTests.Sparrow
{
    public class ZFastTrieTests
    {
        private readonly Func<string, BitVector> binarize = x => BitVector.Of(true, Encoding.UTF8.GetBytes(x));
        
        [Fact]
        public void Construction()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.Equal(0, tree.Count);
            Assert.Null(tree.FirstKeyOrDefault());
            Assert.Null(tree.LastKeyOrDefault());

            ZFastTrieDebugHelpers.StructuralVerify(tree);
        }


        [Fact]
        public void Operations_SingleElement()
        {
            var key = "oren";

            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.True(tree.Add(key, "eini"));
            Assert.Equal(key, tree.FirstKey());
            Assert.Equal(key, tree.LastKey());
            Assert.True(tree.Contains(key));

            string value;
            Assert.True(tree.TryGet(key, out value));

            // x+ = min{y ? S | y = x} (the successor of x in S) - Page 160 of [1]
            // Therefore the successor of the key "oren" is greater or equal to "oren"
            Assert.Equal(key, tree.SuccessorOrDefault(key));
            Assert.Null(tree.SuccessorOrDefault("qu"));

            // x- = max{y ? S | y < x} (the predecessor of x in S) - Page 160 of [1] 
            // Therefore the predecessor of the key "oren" is strictly less than "oren".
            Assert.Null(tree.PredecessorOrDefault(key));
            Assert.Null(tree.PredecessorOrDefault("aq"));
            Assert.Equal(key, tree.PredecessorOrDefault("pq"));

            ZFastTrieDebugHelpers.StructuralVerify(tree);
        }

        [Fact]
        public void Structure_SingleElement()
        {
            var key = "oren";

            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.True(tree.Add(key, "eini"));

            var successor = tree.SuccessorInternal(key);
            Assert.True(successor.IsLeaf);
            Assert.Null(successor.Next.Key);
            Assert.Null(successor.Previous.Key);
            Assert.Equal(tree.Head, successor.Previous);
            Assert.Equal(tree.Tail, successor.Next);

            Assert.Equal(key, successor.Key);

            var predecessor = tree.PredecessorInternal("yy");
            Assert.True(predecessor.IsLeaf);
            Assert.Null(predecessor.Next.Key);
            Assert.Equal(tree.Head, predecessor.Previous);
            Assert.Equal(tree.Tail, predecessor.Next);
            Assert.Null(predecessor.Previous.Key);
            Assert.Equal(key, predecessor.Key);
                        
            Assert.Equal(predecessor, successor);
            Assert.Equal(tree.Root, predecessor);

            ZFastTrieDebugHelpers.StructuralVerify(tree);
        }

        [Fact]
        public void Operations_SingleBranchInsertion()
        {
            string smallestKey = "Ar";
            string lesserKey = "Oren";
            string greaterKey = "oren";
            string greatestKey = "zz";

            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.True(tree.Add(lesserKey, "eini"));
            ZFastTrieDebugHelpers.DumpTree(tree);
            Assert.True(tree.Add(greaterKey, "Eini"));
            ZFastTrieDebugHelpers.DumpTree(tree);

            ZFastTrieDebugHelpers.StructuralVerify(tree);

            Assert.Equal(lesserKey, tree.FirstKey());
            Assert.Equal(greaterKey, tree.LastKey());

            Assert.True(tree.Contains(greaterKey));
            Assert.True(tree.Contains(lesserKey));

            string value;
            Assert.True(tree.TryGet(lesserKey, out value));
            Assert.True(tree.TryGet(greaterKey, out value));
            Assert.False(tree.TryGet(greaterKey + "1", out value));
            Assert.False(tree.TryGet("1", out value));

            // x+ = min{y ? S | y = x} (the successor of x in S) - Page 160 of [1]
            // Therefore the successor of the key "oren" is greater or equal to "oren"
            Assert.Equal(lesserKey, tree.SuccessorOrDefault(lesserKey));
            Assert.Equal(greaterKey, tree.SuccessorOrDefault(greaterKey));
            Assert.Equal(greaterKey, tree.SuccessorOrDefault(lesserKey + "1"));
            Assert.Null(tree.SuccessorOrDefault(greatestKey));

            // x- = max{y ? S | y < x} (the predecessor of x in S) - Page 160 of [1] 
            // Therefore the predecessor of the key "oren" is strictly less than "oren".
            Assert.Equal(lesserKey, tree.PredecessorOrDefault(greaterKey));
            Assert.Null(tree.PredecessorOrDefault(lesserKey));
            Assert.Null(tree.PredecessorOrDefault(smallestKey));
        }

        [Fact]
        public void Structure_SingleBranchInsertion()
        {
            string lesserKey = "Oren";
            string midKey = "aa";
            string greaterKey = "oren";

            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.True(tree.Add(lesserKey, "eini"));
            Assert.True(tree.Add(greaterKey, "Eini"));

            Assert.True(tree.Root.IsInternal);

            var successor = tree.SuccessorInternal(midKey);
            Assert.True(successor.IsLeaf);
            Assert.Null(successor.Next.Key);
            Assert.NotNull(successor.Previous.Key);
            Assert.Equal(tree.Tail, successor.Next);

            var predecessor = tree.PredecessorInternal(midKey);
            Assert.True(predecessor.IsLeaf);
            Assert.NotNull(predecessor.Next.Key);
            Assert.Equal(tree.Head, predecessor.Previous);
            Assert.Null(predecessor.Previous.Key);

            Assert.Equal(predecessor.Next, successor);
            Assert.Equal(successor.Previous, predecessor);

            ZFastTrieDebugHelpers.StructuralVerify(tree);
        }

        [Fact]
        public void Structure_SingleBranchDeletion()
        {
            string lesserKey = "Oren";
            string greaterKey = "oren";

            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            Assert.True(tree.Add(lesserKey, "eini"));
            Assert.True(tree.Add(greaterKey, "Eini"));

            Assert.True(tree.Remove(lesserKey));
            ZFastTrieDebugHelpers.StructuralVerify(tree);

            Assert.True(tree.Remove(greaterKey));
            ZFastTrieDebugHelpers.StructuralVerify(tree);            
        }

        [Fact]
        public void Structure_MultipleBranchInsertion()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);

            Assert.True(tree.Add("8Jp3", "8Jp"));
            Assert.True(tree.Add("GX37", "GX3"));
            Assert.True(tree.Add("f04o", "f04"));
            Assert.True(tree.Add("KmGx", "KmG"));

            ZFastTrieDebugHelpers.StructuralVerify(tree);
            Assert.Equal(4, tree.Count);
        }
        [Fact]
        public void Structure_MultipleBranchDeletion()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);

            Assert.True(tree.Add("8Jp3", "8Jp"));
            Assert.True(tree.Add("GX37", "GX3"));
            Assert.True(tree.Add("f04o", "f04"));
            Assert.True(tree.Add("KmGx", "KmG"));
            Assert.True(tree.Remove("8Jp3"));
            Assert.True(tree.Remove("GX37"));
            Assert.True(tree.Remove("f04o"));
            Assert.True(tree.Remove("KmGx"));

            ZFastTrieDebugHelpers.StructuralVerify(tree);

            Assert.Equal(0, tree.Count);
        }

        [Fact]
        public void Structure_MultipleBranchDeletion2()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);

            Assert.True(tree.Add("CDb", "8J3"));
            Assert.True(tree.Add("tCK", "GX7"));
            Assert.True(tree.Add("B25", "f0o"));
            Assert.True(tree.Add("2mW", "Kmx"));
            Assert.True(tree.Add("gov", string.Empty));
            ZFastTrieDebugHelpers.DumpTree(tree);

            Assert.True(tree.Remove("CDb"));
            ZFastTrieDebugHelpers.DumpTree(tree);
            ZFastTrieDebugHelpers.StructuralVerify(tree);

            Assert.Equal(4, tree.Count);
        }

        [Fact]
        public void Structure_MultipleBranchDeletion3()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);

            Assert.True(tree.Add("0tA", "0A"));
            Assert.True(tree.Add("UUa", "Ua"));
            Assert.True(tree.Add("0b5", "05"));
            Assert.True(tree.Add("8ll", "8l"));
            ZFastTrieDebugHelpers.DumpTree(tree);

            Assert.True(tree.Remove("0tA"));
            ZFastTrieDebugHelpers.DumpTree(tree);
            ZFastTrieDebugHelpers.StructuralVerify(tree);
        }

        [Fact]
        public void Structure_MultipleBranch_OrderPreservation()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);

            tree.Add("8Jp3", "8Jp3");
            tree.Add("V6sl", "V6sl");
            tree.Add("GX37", "GX37");
            tree.Add("f04o", "f04o");
            tree.Add("KmGx", "KmGx");

            ZFastTrieDebugHelpers.StructuralVerify(tree);
            Assert.Equal(5, tree.Count);
        }

        [Fact]
        public void Structure_MultipleBranch_OrderPreservation2()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);

            tree.Add("1Z", "8Jp3");
            tree.Add("fG", "V6sl");
            tree.Add("dW", "GX37");
            tree.Add("8I", "f04o");
            tree.Add("7H", "KmGx");
            tree.Add("73", "KmGx");

            ZFastTrieDebugHelpers.StructuralVerify(tree);
            Assert.Equal(6, tree.Count);
        }

        [Fact]
        public void Structure_MultipleBranch_Simple()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);

            tree.Add("a", "8Jp3");
            tree.Add("c", "V6sl");
            tree.Add("h", "GX37");
            tree.Add("b", "KmGx");

            ZFastTrieDebugHelpers.DumpTree(tree);
            ZFastTrieDebugHelpers.StructuralVerify(tree);

            Assert.Equal(4, tree.Count);
        }

        [Fact]
        public void Structure_MultipleBranch_OrderPreservation3()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);

            tree.Add("6b", "8Jp3");
            tree.Add("ab", "V6sl");
            tree.Add("dG", "GX37");
            tree.Add("3s", "f04o");
            tree.Add("8u", "KmGx");
            tree.Add("cI", "KmGx");

            ZFastTrieDebugHelpers.StructuralVerify(tree);

            Assert.Equal(6, tree.Count);
        }

        [Fact]
        public void Structure_MultipleBranch_InternalExtent()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            tree.Add("8Jp3V6sl", "8Jp3");
            ZFastTrieDebugHelpers.DumpTree(tree);
            tree.Add("VJ7hXe8d", "V6sl");
            ZFastTrieDebugHelpers.DumpTree(tree);
            tree.Add("39XCGX37", "GX37");
            ZFastTrieDebugHelpers.DumpTree(tree);
            tree.Add("f04oKmGx", "f04o");
            ZFastTrieDebugHelpers.DumpTree(tree);
            tree.Add("feiF1gdt", "KmGx");
            ZFastTrieDebugHelpers.DumpTree(tree);

            ZFastTrieDebugHelpers.StructuralVerify(tree);
            Assert.Equal(5, tree.Count);
        }

        [Fact]
        public void Hashing_CalculatePartial_Issue()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);

            BitVector v1 = BitVector.Parse("000000000110");
            var v1State = Hashing.Iterative.XXHash32.Preprocess(v1.Bits);

            BitVector v2 = BitVector.Parse("0000000000110");
            var v2State = Hashing.Iterative.XXHash32.Preprocess(v2.Bits);

            uint v1Hash = ZFastTrieSortedSet<string, string>.ZFastNodesTable.CalculateHashForBits(v1, v1State, 12);
            uint v2Hash = ZFastTrieSortedSet<string, string>.ZFastNodesTable.CalculateHashForBits(v2, v2State, 13);

            Assert.NotEqual(v1Hash, v2Hash);


            v1 = BitVector.Of(0xDEAD, 0xBEEF, 0xDEAD);
            v1State = Hashing.Iterative.XXHash32.Preprocess(v1.Bits);

            v2 = BitVector.Of(0xDEAD, 0xBEEF, 0xBEAF);
            v2State = Hashing.Iterative.XXHash32.Preprocess(v2.Bits);

            v1Hash = ZFastTrieSortedSet<string, string>.ZFastNodesTable.CalculateHashForBits(v1, v1State, v1.Count);
            v2Hash = ZFastTrieSortedSet<string, string>.ZFastNodesTable.CalculateHashForBits(v2, v2State, v2.Count);

            Assert.NotEqual(v1Hash, v2Hash);

        }

        [Fact]
        public void Structure_NodesTable_FailedTableVerify()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            tree.Add("R", "1q");
            tree.Add("F", "3n");
            tree.Add("O", "6e");
            tree.Add("E", "Fs");
            tree.Add("Lr", "LD");
            tree.Add("L5", "MU");

            ZFastTrieDebugHelpers.StructuralVerify(tree);
            Assert.Equal(6, tree.Count);
        }

        [Fact]
        public void Structure_MultipleBranch_InternalExtent2()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);
            tree.Add("i", "8Jp3");
            tree.Add("4", "V6sl");
            tree.Add("j", "GX37");
            tree.Add("P", "f04o");
            tree.Add("8", "KmGx");
            tree.Add("3", "KmG3");

            ZFastTrieDebugHelpers.StructuralVerify(tree);
            Assert.Equal(6, tree.Count);
        }

        [Fact]
        public void Addition_FailureToPass_QuickPath()
        {
            var tree = new ZFastTrieSortedSet<string, string>(binarize);

            tree.Add("lJCn3J", string.Empty);
            tree.Add("4wLolJ", string.Empty);
            tree.Add("FZt4Dp", string.Empty);
            tree.Add("8NSagc", string.Empty);
            tree.Add("9eI05C", string.Empty);
            tree.Add("C4gnS4", string.Empty);
            tree.Add("PRjxjs", string.Empty);
            tree.Add("3M7Oxy", string.Empty);
            tree.Add("boKWpa", string.Empty);
            tree.Add("FLnjoZ", string.Empty);
            tree.Add("AE1Jlq", string.Empty);
            tree.Add("mbHypw", string.Empty);
            tree.Add("FLnjhT", string.Empty);
            tree.Add("fvrTYR", string.Empty);
            tree.Add("2pOGiH", string.Empty);
            tree.Add("RpmKwf", string.Empty);
            tree.Add("1ulQmV", string.Empty);
            tree.Add("rn8YRe", string.Empty);
            tree.Add("wfnTE2", string.Empty);
            tree.Add("rqqjR5", string.Empty);

            ZFastTrieDebugHelpers.StructuralVerify(tree);
            Assert.Equal(20, tree.Count);
        }

        private static readonly string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        private static string GenerateRandomString(Random generator, int size)
        {           
            var stringChars = new char[size];         
            for (int i = 0; i < stringChars.Length; i++)
                stringChars[i] = chars[generator.Next(chars.Length)];

            return new String(stringChars);
        }


        public static IEnumerable<object[]> TreeSize
        {
            get
            {
                // Or this could read from a file. :)
                return new[]
                {
                    new object[] { 102, 4, 4 },                    
                    new object[] { 100, 4, 8 },
                    new object[] { 101, 2, 128 },
                    new object[] { 100, 8, 5 },
                    new object[] { 100, 16, 168 }
                };
            }
        }

        [Fact]
        public void Structure_RandomTester()
        {            
            int count = 1000;
            int size = 5;
            for (int i = 0; i < 1; i++)
            {
                var keys = new string[count];

                var tree = new ZFastTrieSortedSet<string, string>(binarize);

                var insertedKeys = new HashSet<string>();

                var generator = new Random(i + size);
                for (int j = 0; j < count; j++)
                {
                    string key = GenerateRandomString(generator, size);

                    if (!tree.Contains(key))
                        tree.Add(key, key);

                    keys[j] = key;
                    insertedKeys.Add(key);
                }

                ZFastTrieDebugHelpers.StructuralVerify(tree);

                generator = new Random(i + size + 1);
                for (int j = 0; j < count; j++)
                {
                    string key = GenerateRandomString(generator, size);

                    if (!insertedKeys.Contains(key))
                        Assert.False(tree.Remove(key));          
                }

                generator = new Random(i + size);
                for (int j = 0; j < count; j++)
                {
                    string key = GenerateRandomString(generator, size);

                    bool removed = tree.Remove(key);
                    Assert.True(removed);                    
                }

                Assert.Equal(0, tree.Count);
            }
        }

        [Theory, MemberData("TreeSize")]
        public void Structure_CappedSizeInsertion(int seed, int size, int count)
        {
            var generator = new Random(seed);

            var tree = new ZFastTrieSortedSet<string, string>(binarize);

            var keys = new string[count];
            for (int i = 0; i < count; i++)
            {
                string key = GenerateRandomString(generator, size);

                if (!tree.Contains(key))
                    tree.Add(key, key);

                keys[i] = key;
            }

            ZFastTrieDebugHelpers.StructuralVerify(tree);
        }

    }

    public static class ZFastTrieDebugHelpers
    {

        public static void DumpKeys<T, W>(ZFastTrieSortedSet<T, W> tree, TextWriter writer) where T : IEquatable<T>
        {

            writer.WriteLine("Tree stored order");

            var current = tree.Head.Next;
            while (current != null && current != tree.Tail)
            {
                writer.WriteLine(current.Key.ToString());
                current = current.Next;
            }
        }

        public static void DumpTree<T, W>(ZFastTrieSortedSet<T, W> tree) where T : IEquatable<T>
        {
            if (Debugger.IsAttached == false)
                return;

            if (tree.Count == 0)
            {
                Console.WriteLine("Tree is empty.");
            }
            else
            {
                DumpNodes(tree, tree.Root, null, 0, 0);
            }
        }

        private static int DumpNodes<T, W>(ZFastTrieSortedSet<T, W> tree, ZFastTrieSortedSet<T, W>.Node node, ZFastTrieSortedSet<T, W>.Node parent, int nameLength, int depth) where T : IEquatable<T>
        {

            if (Debugger.IsAttached == false)
                return 0;

            if (node == null)
                return 0;

            for (int i = depth; i-- != 0; )
                Console.Write('\t');

            if (node is ZFastTrieSortedSet<T, W>.Internal)
            {
                var internalNode = node as ZFastTrieSortedSet<T, W>.Internal;

                Console.WriteLine(string.Format("Node {0} (name length: {1}) Jump left: {2} Jump right: {3}", node.ToDebugString(tree), nameLength, internalNode.JumpLeftPtr.ToDebugString(tree), internalNode.JumpRightPtr.ToDebugString(tree)));

                return 1 + DumpNodes(tree, internalNode.Left, internalNode, internalNode.ExtentLength + 1, depth + 1)
                         + DumpNodes(tree, internalNode.Right, internalNode, internalNode.ExtentLength + 1, depth + 1);
            }
            else
            {
                Console.WriteLine(string.Format("Node {0} (name length: {1})", node.ToDebugString(tree), nameLength));

                return 1;
            }
        }


        public static void StructuralVerify<T, W>(ZFastTrieSortedSet<T, W> tree) where T : IEquatable<T>
        {
            Assert.NotNull(tree.Head);
            Assert.NotNull(tree.Tail);
            Assert.Null(tree.Tail.Next);
            Assert.Null(tree.Head.Previous);

            Assert.True(tree.Root == null || tree.Root.NameLength == 0); // Either the root does not exist or the root is internal and have name length == 0
            Assert.True(tree.Count == 0 && tree.NodesTable.Count == 0 || tree.Count == tree.NodesTable.Values.Count() + 1); 

            if (tree.Count == 0)
            {
                Assert.Equal(tree.Head, tree.Tail.Previous);
                Assert.Equal(tree.Tail, tree.Head.Next);

                Assert.NotNull(tree.NodesTable);
                Assert.Equal(0, tree.NodesTable.Count);

                return; // No more to check for an empty trie.
            }

            var root = tree.Root;
            var nodes = new HashSet<ZFastTrieSortedSet<T, W>.Node>();

            foreach (var node in tree.NodesTable.Values)
            {
                int handleLength = node.GetHandleLength(tree);

                Assert.True(root == node || root.GetHandleLength(tree) < handleLength); // All handled of lower nodes must be bigger than the root.
                Assert.Equal(node, node.ReferencePtr.ReferencePtr); // The reference of the reference should be itself.

                nodes.Add(node);
            }

            Assert.Equal(tree.NodesTable.Values.Count(), nodes.Count); // We are ensuring there are no repeated nodes in the hash table. 

            if (tree.Count == 1)
            {
                Assert.Equal(tree.Root, tree.Head.Next);
                Assert.Equal(tree.Root, tree.Tail.Previous);
            }
            else
            {
                var toRight = tree.Head.Next;
                var toLeft = tree.Tail.Previous;

                for (int i = 1; i < tree.Count; i++)
                {
                    // Ensure there is name order in the linked list of leaves.
                    Assert.True(toRight.Name(tree).CompareTo(toRight.Next.Name(tree)) <= 0);
                    Assert.True(toLeft.Name(tree).CompareTo(toLeft.Previous.Name(tree)) >= 0);

                    toRight = toRight.Next;
                    toLeft = toLeft.Previous;
                }

                var leaves = new HashSet<ZFastTrieSortedSet<T, W>.Leaf>();
                var references = new HashSet<T>();

                int numberOfNodes = VisitNodes(tree, tree.Root, null, 0, nodes, leaves, references);
                Assert.Equal(2 * tree.Count - 1, numberOfNodes); // The amount of nodes is directly correlated with the tree size.
                Assert.Equal(tree.Count, leaves.Count); // The size of the tree is equal to the amount of leaves in the tree.

                int counter = 0;
                foreach (var leaf in leaves)
                {
                    if (references.Contains(leaf.Key))
                        counter++;
                }

                Assert.Equal(tree.Count - 1, counter);
            }

            Assert.Equal(0, nodes.Count);

            tree.NodesTable.VerifyStructure();
        }

        private static int VisitNodes<T, W>(ZFastTrieSortedSet<T, W> tree, ZFastTrieSortedSet<T, W>.Node node,
                                     ZFastTrieSortedSet<T, W>.Node parent, int nameLength,
                                     HashSet<ZFastTrieSortedSet<T, W>.Node> nodes,
                                     HashSet<ZFastTrieSortedSet<T, W>.Leaf> leaves,
                                     HashSet<T> references) where T : IEquatable<T>
        {
            if (node == null)
                return 0;

            Assert.True(nameLength <= node.GetExtentLength(tree));

            var parentAsInternal = parent as ZFastTrieSortedSet<T, W>.Internal;
            if (parentAsInternal != null)
                Assert.True(parent.Extent(tree).Equals(node.Extent(tree).SubVector(0, parentAsInternal.ExtentLength)));

            if (node is ZFastTrieSortedSet<T, W>.Internal)
            {
                var leafNode = node.ReferencePtr as ZFastTrieSortedSet<T, W>.Leaf;
                Assert.NotNull(leafNode); // We ensure that internal node references are leaves. 

                Assert.True(references.Add(leafNode.Key));
                Assert.True(nodes.Remove(node));

                var handle = node.Handle(tree);

                var allNodes = tree.NodesTable.Values
                                              .Select(x => x.Handle(tree));

                Assert.True(allNodes.Contains(handle));

                var internalNode = (ZFastTrieSortedSet<T, W>.Internal)node;
                int jumpLength = internalNode.GetJumpLength(tree);

                var jumpLeft = internalNode.Left;
                while (jumpLeft is ZFastTrieSortedSet<T, W>.Internal && jumpLength > ((ZFastTrieSortedSet<T, W>.Internal)jumpLeft).ExtentLength)
                    jumpLeft = ((ZFastTrieSortedSet<T, W>.Internal)jumpLeft).Left;

                Assert.Equal(internalNode.JumpLeftPtr, jumpLeft);

                var jumpRight = internalNode.Right;
                while (jumpRight is ZFastTrieSortedSet<T, W>.Internal && jumpLength > ((ZFastTrieSortedSet<T, W>.Internal)jumpRight).ExtentLength)
                    jumpRight = ((ZFastTrieSortedSet<T, W>.Internal)jumpRight).Right;

                Assert.Equal(internalNode.JumpRightPtr, jumpRight);

                return 1 + VisitNodes(tree, internalNode.Left, internalNode, internalNode.ExtentLength + 1, nodes, leaves, references)
                         + VisitNodes(tree, internalNode.Right, internalNode, internalNode.ExtentLength + 1, nodes, leaves, references);
            }
            else
            {
                var leafNode = node as ZFastTrieSortedSet<T, W>.Leaf;

                Assert.NotNull(leafNode);
                Assert.True(leaves.Add(leafNode)); // We haven't found this leaf somewhere else.
                Assert.Equal(leafNode.Name(tree).Count, leafNode.GetExtentLength(tree)); // This is a leaf, the extent is the key

                Assert.True(parent.ReferencePtr is ZFastTrieSortedSet<T, W>.Leaf); // We ensure that internal node references are leaves. 

                return 1;
            }
        }       
    }
}

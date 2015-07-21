using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Collections
{
    /// <summary>
    /// In-Memory Dynamic Z-Fast TRIE supporting predecessor, successor using low linear additional space. 
    /// "A dynamic z-fast trie is a compacted trie endowed with two additional pointers per internal node and with a 
    /// dictionary. [...] it can be though of as an indexing structure built on a set of binary strings S. 
    /// 
    /// As described in "Dynamic Z-Fast Tries" by Belazzougui, Boldi and Vigna in String Processing and Information
    /// Retrieval. Lecture notes on Computer Science. Volume 6393, 2010, pp 159-172 [1]
    /// </summary>
    public sealed class ZFastTrieSortedSet<TKey, TValue> where TKey : IEquatable<TKey>
    {
        private abstract class Node
        {
            public readonly int NameLength;

            public abstract bool IsLeaf { get; }
            public abstract bool IsInternal { get; }

            protected Node ()
            {}

            public Node(int nameLength)
            {
                this.NameLength = nameLength;
            }

            /// <summary>
            /// In the leaf it is the pointer to the nearest internal cut node. In the internal node, it is a pointer to any leaf 
            /// in the subtree, as all leaves will share the same key prefix.
            /// </summary>
            public Node ReferencePtr;

            /// <summary>
            /// The name of a node, is the string deprived of the string stored at it. Page 163 of [1]
            /// </summary>
            public abstract BitVector Name(Func<TKey, BitVector> binarizeFunc);

            /// <summary>
            /// The handle of a node is the prefix of the name whose length is 2-fattest number in the skip interval of it. If the
            /// skip interval is empty (which can only happen at the root) we define the handle to be the empty string/vector.
            /// </summary>
            public abstract BitVector Handle(Func<TKey, BitVector> binarizeFunc);

            /// <summary>
            /// The extent of a node, is the longest common prefix of the strings represented by the leaves that are descendants of it.
            /// </summary>            
            public abstract BitVector Extent(Func<TKey, BitVector> binarizeFunc);

            public Leaf GetRightLeaf()
            {
                if (this is Leaf)
                    return (Leaf)this;

                Node node = this;
                do
                {
                    node = ((Internal)node).JumpRightPtr;
                }
                while (node is Internal);

                return (Leaf)node;
            }

            public Leaf GetLeftLeaf()
            {
                if (this is Leaf)
                    return (Leaf)this;

                Node node = this;
                do
                {
                    node = ((Internal)node).JumpLeftPtr;
                }
                while (node is Internal);

                return (Leaf)node;
            }
        }

        /// <summary>
        /// Leaves are organized in a double linked list: each leaf, besides a pointer to the corresponding string of S, 
        /// stores two pointers to the next/previous leaf in lexicographic order. Page 163 of [1].
        /// </summary>
        private sealed class Leaf : Node
        {
            public override bool IsLeaf { get { return true; } }
            public override bool IsInternal { get { return false; } }


            public Leaf()
            {
            }

            public Leaf(int nameLength, TKey key, TValue value)
                : base(nameLength)
            {
                this.Key = key;
                this.Value = value;
            }

            /// <summary>
            /// The previous leaf in the double linked list referred in page 163 of [1].
            /// </summary>
            public Leaf Previous;

            /// <summary>
            /// The public leaf in the double linked list referred in page 163 of [1].
            /// </summary>

            public Leaf Next;

            public TKey Key;

            public TValue Value;


            public override BitVector Name(Func<TKey, BitVector> binarizeFunc)
            {
                return binarizeFunc(this.Key);
            }

            public override BitVector Handle(Func<TKey, BitVector> binarizeFunc)
            {
                return binarizeFunc(this.Key);
            }

            public override BitVector Extent(Func<TKey, BitVector> binarizeFunc)
            {
                return Name(binarizeFunc);
            }
        }


        /// <summary>
        /// Every internal node contains a pointer to its two children, the extremes ia and ja of its skip interval,
        /// its own extent ea and two additional jump pointers J- and J+. Page 163 of [1].
        /// </summary>
        private sealed class Internal : Node
        {
            public readonly int ExtentLength;

            public override bool IsLeaf { get { return false; } }
            public override bool IsInternal { get { return true; } }            

            public Internal(int nameLength, int extentLength)
                : base(nameLength)
            {
                this.ExtentLength = extentLength;
            }

            /// <summary>
            /// The right subtrie.
            /// </summary>
            public Node Right;

            /// <summary>
            /// The left subtrie.
            /// </summary>
            public Node Left;

            /// <summary>
            /// The downward right jump pointer.
            /// </summary>
            public Node JumpRightPtr;

            /// <summary>
            /// The downward left jump pointer.
            /// </summary>
            public Node JumpLeftPtr;

            public override BitVector Name(Func<TKey, BitVector> binarizeFunc)
            {
                return ReferencePtr.Name(binarizeFunc);
            }

            public override BitVector Handle(Func<TKey, BitVector> binarizeFunc)
            {
                int handleLength = ZFastTrieSortedSet<TKey,TValue>.TwoFattest(NameLength - 1, this.ExtentLength);

                return ReferencePtr.Name(binarizeFunc).SubVector(0, handleLength);
            }

            public override BitVector Extent(Func<TKey, BitVector> binarizeFunc)
            {
                return ReferencePtr.Name(binarizeFunc).SubVector(0, this.ExtentLength);
            }
        }

        private class CutPoint
        {
            /// <summary>
            /// Longest Common Prefix (or LCP) between the Exit(x) and x
            /// </summary>
            public readonly int LongestPrefix;
            /// <summary>
            /// The parent of the exit node.
            /// </summary>
            public readonly Internal Parent;
            /// <summary>
            /// The exit node. If parex(x) == root then exit(x) is the root; otherwise, exit(x) is the left or right child of parex(x) 
            /// depending whether x[|e-parex(x)|] is zero or one, respectively. Page 166 of [1]
            /// </summary>
            public readonly Node Exit;

            public CutPoint(int lcp, Internal parent, Node exit)
            {
                this.LongestPrefix = lcp;
                this.Parent = parent;
                this.Exit = exit;
            }

            public bool IsCutLow(Func<TKey, BitVector> binarizeFunc)
            {
                // Theorem 3: Page 165 of [1]
                int handleLength = this.Exit.Handle(binarizeFunc).Count;
                return this.LongestPrefix >= handleLength;
            }

            public bool IsRightChild
            {
                get { return this.Parent != null && this.Parent.Right == this.Exit; }
            }
        }

        private class ExitNode
        {
            /// <summary>
            /// Longest Common Prefix (or LCP) between the Exit(x) and x
            /// </summary>
            public readonly int LongestPrefix;

            /// <summary>
            /// The exit node, it will be a leaf when the search key matches the query. 
            /// </summary>
            public readonly Node Exit;

            public readonly BitVector SearchKey;

            public ExitNode(int lcp, Node exit, BitVector v)
            {
                this.LongestPrefix = lcp;
                this.Exit = exit;
                this.SearchKey = v;
            }

            public bool IsLeaf
            {
                get { return this.Exit is Leaf; }
            }
        }

        private readonly Func<TKey, BitVector> binarizeFunc;

        private int size;
        private readonly Leaf head;
        private readonly Leaf tail;
        private Node root;

        public ZFastTrieSortedSet(Func<TKey, BitVector> binarize)
        {
            if (binarize == null)
                throw new ArgumentException("Cannot continue without knowing how to binarize the key.");

            this.binarizeFunc = binarize;

            this.size = 0;
            this.root = null;

            // Setup tombstones. 
            this.head = new Leaf();
            this.tail = new Leaf();
            this.head.Next = tail;
            this.tail.Previous = head;
        }

        public ZFastTrieSortedSet(IEnumerable<KeyValuePair<TKey, TValue>> elements, Func<TKey, BitVector> binarize) : this ( binarize )
        {
            if (elements == null)
                throw new NullReferenceException();

            foreach ( var element in elements )
                Add(element.Key, element.Value);
        }

        public int Count { get; private set; }

        public bool Add(TKey key, TValue value)
        {
            if ( size == 0 )
            {
                // We set the root of the current key to the new leaf.
                Leaf leaf = new Leaf
                {
                    Key = key,
                };                

                // We add the leaf after the head.
                AddAfter(this.head, leaf);
                this.root = leaf;

                size++;

                return true;
            }

            // We prepare the signature to compute incrementally. 
            BitVector v = binarizeFunc(key);
            var hashState = Hashing.Iterative.XXHash32.Preprocess(v.Bits);
            
            // We look for the parent of the exit node for the key.
            Stack<Node> stack = new Stack<Node>();
            var cutPoint = FindParentExitNode(v, stack, hashState);                                                       

            // If the exit node is a leaf and the key is equal to the LCP 
            var exitNodeAsLeaf = cutPoint.Exit as Leaf;
            if (exitNodeAsLeaf != null && binarizeFunc(exitNodeAsLeaf.Key).Count == cutPoint.LongestPrefix)
                return false; // Then we are done (we found the key already).

            var exitNodeAsInternal = cutPoint.Exit as Internal; // Is the exit node internal?
            bool isCutLow = cutPoint.IsCutLow (binarizeFunc);  // Is this cut point low or high? 
            bool exitDirection = v.Get(cutPoint.LongestPrefix);   // Compute the exit direction from the LCP.

            // Create a new internal node that will hold the new leaf.            
            var newLeaf = new Leaf(cutPoint.LongestPrefix + 1, key, value);
            var newInternal = new Internal(cutPoint.Exit.NameLength, cutPoint.LongestPrefix);

            newInternal.ReferencePtr = newLeaf;
            newLeaf.ReferencePtr = newInternal;            

            // Link the internal and the leaf according to its exit direction.
            if ( exitDirection )
            {
                newInternal.Right = newLeaf;
                newInternal.JumpRightPtr = newLeaf;                

                newInternal.Left = cutPoint.Exit;                
                newInternal.JumpLeftPtr = isCutLow && exitNodeAsInternal != null ? exitNodeAsInternal.JumpLeftPtr : cutPoint.Exit;  
            }
            else
            {
                newInternal.Left = newLeaf;
                newInternal.JumpLeftPtr = newLeaf;
                
                newInternal.Right = cutPoint.Exit;
                newInternal.JumpRightPtr = isCutLow && exitNodeAsInternal != null ? exitNodeAsInternal.JumpRightPtr : cutPoint.Exit;  
            }                       

            // If the exit node is the root
            bool isRightChild = cutPoint.IsRightChild;
            if ( cutPoint.Exit == this.root)
            {
                // Then update the root
                this.root = newInternal;
            }
            else
            {
                // Else update the parent exit node.
                if ( isRightChild )
                    cutPoint.Parent.Right = newInternal;
                else
                    cutPoint.Parent.Left = newInternal;
            }
                        
            // Update the jump table after the insertion.
            if (exitDirection)
                UpdateRightJumpsAfterInsertion(newInternal, cutPoint.Exit, isRightChild, newLeaf, stack);
            else
                UpdateLeftJumpsAfterInsertion(newInternal, cutPoint.Exit, isRightChild, newLeaf, stack);

            // If the cut point was low and the exit node internal
            if ( isCutLow && exitNodeAsInternal != null )
            {
                //  We replace the exit node entry with the new internal node
                //  We add a new exit node entry
                //  We update the jumps for the exit node.                
                throw new NotImplementedException();
            }
            else
            {
                //  We add the internal node to the jump table.                
                throw new NotImplementedException();
            }


            // Link the new leaf with it's predecessor and successor.
            if ( exitDirection )
                AddAfter ( cutPoint.Exit.GetRightLeaf(), newLeaf );
            else
                AddBefore( cutPoint.Exit.GetLeftLeaf(), newLeaf);

            size++;

            return true;
        }

        /// <summary>
        /// Deletion mostly follow the insertion steps uneventfully. [...] To fix the jump pointers, we need to know
        /// the 2-fat ancestors of the parent of parex(x), not of parex(x). Page 171 of [1].
        /// </summary>
        public bool Remove(TKey key)
        {
            if (size == 0)
                return false;

            if ( size == 1 )
            {                
                // Is the root key (which has to be a Leaf) equal to the one we are looking for?
                var leaf = (Leaf)this.root;
                if (leaf.Key.Equals(key))
                    return false;

                RemoveLeaf(leaf);

                // We remove the root.                
                this.root = null;
                size = 0;
                
                return true;
            }

            // We prepare the signature to compute incrementally.
            BitVector v = binarizeFunc(key);
            var state = Hashing.Iterative.XXHash32.Preprocess(v.Bits); 

            // We look for the parent of the exit node for the key.            
            // If the exit node is not a leaf or the key is not equal to the LCP 
            // Then we are done (The key does not exist).

            // If the parentExitNode is not null and not the root
            // Then we need to fix the grand parent child pointer.

            // If the parent node is the root, then the child becomes the root.

            // If the exit node (which should be a leaf) reference is not null 
            // Then fix the parent and grandparent references.
            // Else set to null the grandparent of the exit node's reference.

            // Delete the leaf and fix it's predecessor and successor references.

            // Update the jump table after the deletion.

            // If the cut point was low and the child is internal
            // Then
            //   We remove the existing child node from the jump table
            //   We replace the parent exit node
            //   We update the jumps table for the child node.
            // Else
            //   We remove the parent node from the jump table.

            size--;

            throw new NotImplementedException();
        }

        public bool TryGet(TKey key, out TValue value)
        {
            if (size == 0)
            {
                value = default(TValue);
                return false;
            }

            // We look for the parent of the exit node for the key.
            var exitNode = FindExitNode(key);

            // If the exit node is a leaf and the key is equal to the LCP 
            var exitNodeAsLeaf = exitNode.Exit as Leaf;
            if (exitNodeAsLeaf != null && binarizeFunc(exitNodeAsLeaf.Key).Count == exitNode.LongestPrefix)
            {
                value = exitNodeAsLeaf.Value;
                return true; // Then we are done (we found the key already).
            }

            value = default(TValue);
            return false;
        }

        public bool Contains(TKey key)
        {
            if (size == 0)
                return false;

            // We look for the parent of the exit node for the key.
            var exitNode = FindExitNode(key);

            // If the exit node is a leaf and the key is equal to the LCP 
            var exitNodeAsLeaf = exitNode.Exit as Leaf;
            if (exitNodeAsLeaf != null && binarizeFunc(exitNodeAsLeaf.Key).Count == exitNode.LongestPrefix)
                return true; // Then we are done (we found the key already).

            return false;
        }

        public TKey Successor(TKey key)
        {
            if (size == 0)
                throw new KeyNotFoundException();

            return SuccessorInternal(key);
        }

        public TKey Predecessor(TKey key)
        {
            if (size == 0)
                throw new KeyNotFoundException();

            return PredecessorInternal(key);
        }

        public TKey FirstKey()
        {
            if (size == 0)
                throw new KeyNotFoundException();

            // Returns the head next key
            return this.head.Next.Key;
        }

        public TKey LastKey()
        {
            if (size == 0)
                throw new KeyNotFoundException();

            // Returns the head next key
            return this.tail.Previous.Key;
        }

        public void Clear()
        {
            this.size = 0;
            this.root = null;
            this.head.Next = this.tail;
            this.tail.Previous = this.head;
        }

        public TKey SuccessorOrDefault(TKey key)
        {
            // The set is empty, there is no successor to any key.
            if (size == 0)
                return default(TKey);

            return SuccessorInternal(key);
        }

        public TKey PredecessorOrDefault(TKey key)
        {
            // The set is empty, there is no predecessor to any key.
            if (size == 0)
                return default(TKey);

            return PredecessorInternal(key);
        }

        public TKey FirstKeyOrDefault()
        {
            if (size == 0)
                return default(TKey);

            // Returns the head next key
            return this.head.Next.Key;
        }

        public TKey LastKeyOrDefault()
        {
            if (size == 0)
                return default(TKey);

            // Returns the head next key
            return this.tail.Previous.Key;
        }



        public void Add(IEnumerable<KeyValuePair<TKey, TValue>> elements)
        {
            // For now use the naive approach. Use hashes later to pack similar elements together.
            foreach (var element in elements)
                Add(element.Key, element.Value);
        }

        public void Remove(IEnumerable<TKey> elements)
        {
            // For now use the naive approach. Use hashes later to pack similar elements together.
            foreach (var element in elements)
                Remove(element);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private TKey PredecessorInternal(TKey key)
        {            
            // x− = max{y ∈ S | y < x} (the predecessor of x in S) - Page 160 of [1]

            // We look for the exit node for the key
            var exitFound = FindExitNode(key);
            var exitNode = exitFound.Exit;

            // We compare the key with the exit node extent.
            int dummy;
            if (exitNode.Extent(binarizeFunc).CompareToInline(exitFound.SearchKey, out dummy) < 0)
            {
                // If the key is greater than the extent, we exit to the right leaf.
                return exitNode.GetRightLeaf().Key;
            }
            else
            {
                // If the key is smaller than the extent, we exit to the left leaf and get the previous leaf.
                return exitNode.GetLeftLeaf().Previous.Key;
            }            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private TKey SuccessorInternal(TKey key)
        {
            // x+ = min{y ∈ S | y ≥ x} (the successor of x in S) - Page 160 of [1]

            // We look for the exit node for the key
            var exitFound = FindExitNode(key);
            var exitNode = exitFound.Exit;

            // We compare the key with the exit node extent.
            int dummy;
            if (exitFound.SearchKey.CompareToInline(exitNode.Extent(binarizeFunc), out dummy) <= 0)
            {
                // If the key is smaller than the extent, we exit to the left leaf.
                return exitNode.GetLeftLeaf().Key;
            }
            else
            {
                // If the key is greater than the extent, we exit to the right leaf and get the next.
                return exitNode.GetRightLeaf().Next.Key;
            }                        
        }

        private void AddAfter(Leaf predecessor, Leaf newNode)
        {
            newNode.Next = predecessor.Next;
            newNode.Previous = predecessor;
            predecessor.Next.Previous = newNode;
            predecessor.Next = newNode;
        }

        private void RemoveLeaf(Leaf node)
        {
            node.Next.Previous = node.Previous;
            node.Previous.Next = node.Next;
        }

        private void AddBefore(Leaf successor, Leaf newNode)
        {
            newNode.Previous = successor.Previous;
            newNode.Next = successor;
            successor.Previous.Next = newNode;
            successor.Previous = newNode;
        }

        private void UpdateRightJumpsAfterInsertion(Internal insertedNode, Node exitNode, bool isRightChild, Leaf insertedLeaf, Stack<Node> stack)
        {
            throw new NotImplementedException();
        }

        private void UpdateLeftJumpsAfterInsertion(Internal insertedNode, Node exitNode, bool isRightChild, Leaf insertedLeaf, Stack<Node> stack)
        {
            throw new NotImplementedException();
        }


        private CutPoint FindParentExitNode(BitVector v, Stack<Node> stack, Hashing.Iterative.XXHash32Block hashState)
        {
            Contract.Requires(size != 0);

            // If there is only a single element, then the exit point is the root.
            if (size == 1)
                return new CutPoint(v.LongestCommonPrefixLength(this.root.Extent(this.binarizeFunc)), null, root);


            throw new NotImplementedException();
        }

        private ExitNode FindExitNode(TKey key)
        {
            Contract.Requires(size != 0);

            // We prepare the signature to compute incrementally. 
            BitVector v = binarizeFunc(key);
            var state = Hashing.Iterative.XXHash32.Preprocess(v.Bits);

            if (size == 1)
                return new ExitNode(v.LongestCommonPrefixLength(this.root.Extent(binarizeFunc)), this.root, v );

            throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int TwoFattest( int a, int b)
        {
            return -1 << Bits.MostSignificantBit(a ^ b) & b;
        }

    }
}

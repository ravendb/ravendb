using Sparrow.Binary;
using System;
using System.Collections.Generic;
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
        protected abstract class Node
        {
            protected int nameLength;

            public abstract bool IsLeaf { get; }
            public abstract bool IsInternal { get; }

            public Node( int nameLength )
            {
                this.nameLength = nameLength;
            }


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
        }

        /// <summary>
        /// Leaves are organized in a double linked list: each leaf, besides a pointer to the corresponding string of S, 
        /// stores two pointers to the next/previous leaf in lexicographic order. Page 163 of [1].
        /// </summary>
        protected sealed class Leaf : Node
        {
            public override bool IsLeaf { get { return true; } }
            public override bool IsInternal { get { return false; } }

            public Leaf() : base(0)
            { }

            /// <summary>
            /// The previous leaf in the double linked list referred in page 163 of [1].
            /// </summary>
            public Leaf Previous;

            /// <summary>
            /// The public leaf in the double linked list referred in page 163 of [1].
            /// </summary>

            public Leaf Next;

            public Node Reference;

            public TKey Key;

            public TValue Value;


            public override BitVector Name(Func<TKey, BitVector> binarizeFunc)
            {
                throw new NotImplementedException();
            }

            public override BitVector Handle(Func<TKey, BitVector> binarizeFunc)
            {
                throw new NotImplementedException();
            }

            public override BitVector Extent(Func<TKey, BitVector> binarizeFunc)
            {
                throw new NotImplementedException();
            }
        }


        /// <summary>
        /// Every internal node contains a pointer to its two children, the extremes ia and ja of its skip interval,
        /// its own extent ea and two additional jump pointers J- and J+. Page 163 of [1].
        /// </summary>
        protected sealed class Internal : Node
        {
            public override bool IsLeaf { get { return false; } }
            public override bool IsInternal { get { return true; } }

            public Internal(int nameLength)
                : base(nameLength)
            {}

            /// <summary>
            /// The right subtrie.
            /// </summary>
            public Node Right;

            /// <summary>
            /// The left subtrie.
            /// </summary>
            public Node Left;

            /// <summary>
            /// The reference pointer. This is the leaf whose key this node refers to.
            /// </summary>
            public Leaf ReferencePtr;

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
                throw new NotImplementedException();
            }

            public override BitVector Handle(Func<TKey, BitVector> binarizeFunc)
            {
                throw new NotImplementedException();
            }

            public override BitVector Extent(Func<TKey, BitVector> binarizeFunc)
            {
                throw new NotImplementedException();
            }
        }

        private readonly Func<TKey, BitVector> binarizeFunc;

        private int size;
        private Leaf head;
        private Leaf tail;
        private Node root;


        public ZFastTrieSortedSet(IEnumerable<KeyValuePair<TKey, TValue>> elements, Func<TKey, BitVector> binarize)
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

            Add(elements);
        }

        public int Count { get; private set; }

        public void Add(TKey key, TValue value)
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

                return;
            }

            // We prepare the signature to compute incrementally. 
            BitVector v = binarizeFunc(key);
            var hashState = Hashing.Iterative.XXHash32.Preprocess(v.Bits);
            
            // We look for the parent of the exit node for the key.
            Stack<Node> stack = new Stack<Node>();
            //var parentExitNode = FindParentExitNode(v, stack, hashState);
            
            // If the exit node is a leaf and the key is equal to the LCP 
            // Then we are done (we found the key already).

            // Compute the exit direction from the LCP.

            // Create a new internal node that will hold the new leaf.
            // Link the internal and the leaf according to its exit direction.

            // If the exit node is the root
            // Then update the root
            // Else update the parent exit node.

            // Update the jump table after the insertion.

            // If the cut point was low and the exit node internal
            // Then
            //  We replace the exit node entry with the new internal node
            //  We add a new exit node entry
            //  We update the jumps for the exit node.
            // Else
            //  We add the internal node to the jump table.

            // Link the new leaf with it's predecessor and successor.

            size++;

            throw new NotImplementedException();
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

        public bool TryGet( TKey key, out TValue value )
        {
            // We prepare the signature to compute incrementally. After preprocessing using O(|x|/w) times and O(|x|/B) IOs, 
            // computing a signature for a prefix x takes constant time. Page 167 of [1]
            BitVector v = binarizeFunc(key);
            var state = Hashing.Iterative.XXHash32.Preprocess(v.Bits); 

            throw new NotImplementedException();
        }

        public bool Contains(TKey key)
        {
            // We prepare the signature to compute incrementally. After preprocessing using O(|x|/w) times and O(|x|/B) IOs, 
            // computing a signature for a prefix x takes constant time. Page 167 of [1]
            BitVector v = binarizeFunc(key);
            var state = Hashing.Iterative.XXHash32.Preprocess(v.Bits);

            throw new NotImplementedException();

            // We look for the exit node for the key

            // If the exit node is a leaf and the LCP matches the exit node key, 
            // Then returns true (the key exist). False otherwise.
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

            throw new NotImplementedException();

            // Returns the head next key
        }

        public TKey LastKey()
        {
            throw new NotImplementedException();

            // Returns the tail previous key
        }

        public void Clear()
        {
            throw new NotImplementedException();
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

        public TKey LastKeyOrDefault()
        {
            throw new NotImplementedException();
        }

        public TKey FirstKeyOrDefault()
        {
            throw new NotImplementedException();
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
            // We prepare the signature to compute incrementally. 
            BitVector v = binarizeFunc(key);
            var state = Hashing.Iterative.XXHash32.Preprocess(v.Bits);

            throw new NotImplementedException();

            // We look for the exit node for the key

            // We compare the key with the exit node extent.
            // If the key is smaller than the extent, we exit to the right leaf.
            // If the key is greater than the extent, we exit to the left leaf and get the previous leaf.
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private TKey SuccessorInternal(TKey key)
        {
            // We prepare the signature to compute incrementally. 
            BitVector v = binarizeFunc(key);
            var state = Hashing.Iterative.XXHash32.Preprocess(v.Bits);

            throw new NotImplementedException();

            // We look for the exit node for the key

            // We compare the key with the exit node extent.
            // If the key is smaller than the extent, we exit to the left leaf.
            // If the key is greater than the extent, we exit to the right leaf and get the next.
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
    }
}

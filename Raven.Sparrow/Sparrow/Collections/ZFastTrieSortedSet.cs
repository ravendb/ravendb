using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        internal abstract class Node
        {
            public int NameLength;

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
            public abstract BitVector Name(ZFastTrieSortedSet<TKey, TValue> owner);

            /// <summary>
            /// The handle of a node is the prefix of the name whose length is 2-fattest number in the skip interval of it. If the
            /// skip interval is empty (which can only happen at the root) we define the handle to be the empty string/vector.
            /// </summary>
            public abstract BitVector Handle(ZFastTrieSortedSet<TKey, TValue> owner);

            /// <summary>
            /// The extent of a node, is the longest common prefix of the strings represented by the leaves that are descendants of it.
            /// </summary>            
            public abstract BitVector Extent(ZFastTrieSortedSet<TKey, TValue> owner);

            public int GetHandleLength(ZFastTrieSortedSet<TKey, TValue> owner)
            {
                return ZFastTrieSortedSet<TKey, TValue>.TwoFattest(NameLength - 1, GetExtentLength(owner));
            }

            public abstract int GetExtentLength(ZFastTrieSortedSet<TKey, TValue> owner);

            public bool IsExitNodeOf(ZFastTrieSortedSet<TKey, TValue> owner, int length, int lcpLength)
            {
                return this.NameLength <= lcpLength && (lcpLength < this.GetExtentLength(owner) || lcpLength == length);
            }

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

            public string ToDebugString(ZFastTrieSortedSet<TKey,TValue> owner)
            {
                TKey key = this.IsInternal ? ((Leaf)((Internal)this).ReferencePtr).Key : ((Leaf)this).Key;
                
                BitVector extent = Extent(owner);
                int extentLength = GetExtentLength(owner);

                string openBracket = this.IsLeaf ? "[" : "(" ;
                string closeBracket = this.IsLeaf ? "]" : ")";
                string extentBinary = extentLength > 16 ? extent.SubVector(0, 8).ToBinaryString() + "..." + extent.SubVector(extent.Count - 8, 8).ToBinaryString() : extent.ToBinaryString();
                string lenghtInformation = "[" + this.NameLength + ".." + extentLength + "]";
                string jumpInfo = this.IsInternal ? ((Internal)this).GetHandleLength(owner) + "->" + ((Internal)this).GetJumpLength(owner) : "";

                return string.Format("{0}{2}, {4}, {3}{1}", openBracket, closeBracket, extentBinary, jumpInfo, lenghtInformation);
            }
        }

        /// <summary>
        /// Leaves are organized in a double linked list: each leaf, besides a pointer to the corresponding string of S, 
        /// stores two pointers to the next/previous leaf in lexicographic order. Page 163 of [1].
        /// </summary>
        internal sealed class Leaf : Node
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


            public override BitVector Name(ZFastTrieSortedSet<TKey, TValue> owner)
            {
                return owner.binarizeFunc(this.Key);
            }

            public override BitVector Handle(ZFastTrieSortedSet<TKey, TValue> owner)
            {
                return this.ReferencePtr.Name(owner).SubVector(0, GetHandleLength(owner));
            }

            public override BitVector Extent(ZFastTrieSortedSet<TKey, TValue> owner)
            {
                return Name(owner);
            }

            public override int GetExtentLength(ZFastTrieSortedSet<TKey, TValue> owner)
            {
                return owner.binarizeFunc(this.Key).Count;
            }
        }


        /// <summary>
        /// Every internal node contains a pointer to its two children, the extremes ia and ja of its skip interval,
        /// its own extent ea and two additional jump pointers J- and J+. Page 163 of [1].
        /// </summary>
        internal sealed class Internal : Node
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

            public override BitVector Name(ZFastTrieSortedSet<TKey, TValue> owner)
            {
                return ReferencePtr.Name(owner);
            }

            public override BitVector Handle(ZFastTrieSortedSet<TKey, TValue> owner)
            {
                int handleLength = GetHandleLength(owner);

                return ReferencePtr.Name(owner).SubVector(0, handleLength);
            }

            public override BitVector Extent(ZFastTrieSortedSet<TKey, TValue> owner)
            {
                return ReferencePtr.Name(owner).SubVector(0, this.ExtentLength);
            }

            public override int GetExtentLength(ZFastTrieSortedSet<TKey, TValue> owner)
            {
                return ExtentLength;
            }

            public int GetJumpLength(ZFastTrieSortedSet<TKey, TValue> owner)
            {
                int handleLength = GetHandleLength(owner);
                if (handleLength == 0)
                    return int.MaxValue;

                return handleLength + (handleLength & -handleLength);
            }
        }

        internal class CutPoint
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
            /// The binary representation of the search key.
            /// </summary>
            public readonly BitVector SearchKey;

            /// <summary>
            /// The exit node. If parex(x) == root then exit(x) is the root; otherwise, exit(x) is the left or right child of parex(x) 
            /// depending whether x[|e-parex(x)|] is zero or one, respectively. Page 166 of [1]
            /// </summary>
            public readonly Node Exit;

            public CutPoint(int lcp, Internal parent, Node exit, BitVector searchKey)
            {
                this.LongestPrefix = lcp;
                this.Parent = parent;
                this.Exit = exit;
                this.SearchKey = searchKey;
            }

            public bool IsCutLow(ZFastTrieSortedSet<TKey, TValue> owner)
            {
                // Theorem 3: Page 165 of [1]
                var handleLength = this.Exit.GetHandleLength(owner);
                return this.LongestPrefix >= handleLength;
            }

            public bool IsRightChild
            {
                get { return this.Parent != null && this.Parent.Right == this.Exit; }
            }
        }

        internal class ExitNode
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

        internal readonly Dictionary<uint, List<Internal>> NodesTable = new Dictionary<uint, List<Internal>>();
        internal readonly Leaf Head;
        internal readonly Leaf Tail;
        internal Node Root;


        public ZFastTrieSortedSet(Func<TKey, BitVector> binarize)
        {
            if (binarize == null)
                throw new ArgumentException("Cannot continue without knowing how to binarize the key.");

            this.binarizeFunc = binarize;

            this.size = 0;
            this.Root = null;

            // Setup tombstones. 
            this.Head = new Leaf();
            this.Tail = new Leaf();
            this.Head.Next = Tail;
            this.Tail.Previous = Head;
        }

        public ZFastTrieSortedSet(IEnumerable<KeyValuePair<TKey, TValue>> elements, Func<TKey, BitVector> binarize) : this ( binarize )
        {
            if (elements == null)
                throw new NullReferenceException();

            foreach ( var element in elements )
                Add(element.Key, element.Value);
        }

        public int Count { get { return size; } }
        

        public bool Add(TKey key, TValue value)
        {
            // We prepare the signature to compute incrementally. 
            BitVector searchKey = binarizeFunc(key);
#if DETAILED_DEBUG
            Console.WriteLine(string.Format("Add(Binary: {1}, Key: {0})", key.ToString(), searchKey.ToBinaryString()));
#endif
            if ( size == 0 )
            {
                // We set the root of the current key to the new leaf.
                Leaf leaf = new Leaf(0, key, value);

                // We add the leaf after the head.
                AddAfter(this.Head, leaf);
                this.Root = leaf;

                size++;

                return true;
            }

            var hashState = Hashing.Iterative.XXHash32.Preprocess(searchKey.Bits);
           
            // We look for the parent of the exit node for the key.
            Stack<Internal> stack = new Stack<Internal>();
            var cutPoint = FindParentExitNode(searchKey, stack);

            var exitNode = cutPoint.Exit;

#if DETAILED_DEBUG        
            Console.WriteLine(string.Format("Parex Node: {0}, Exit Node: {1}, LCP: {2}", cutPoint.Parent != null ? cutPoint.Parent.ToDebugString(this) : "null", cutPoint.Exit.ToDebugString(this), cutPoint.LongestPrefix));
#endif

            // If the exit node is a leaf and the key is equal to the LCP 
            var exitNodeAsLeaf = exitNode as Leaf;
            if (exitNodeAsLeaf != null && binarizeFunc(exitNodeAsLeaf.Key).Count == cutPoint.LongestPrefix)
                return false; // Then we are done (we found the key already).

            var exitNodeAsInternal = exitNode as Internal; // Is the exit node internal?  

            int exitNodeHandleLength = exitNode.GetHandleLength(this);
            bool exitDirection = cutPoint.SearchKey.Get(cutPoint.LongestPrefix);   // Compute the exit direction from the LCP.
            bool isCutLow = cutPoint.LongestPrefix >= exitNodeHandleLength;  // Is this cut point low or high? 

#if DETAILED_DEBUG
            Console.WriteLine(string.Format("Cut {0}; exit to the {1}", isCutLow ? "low" : "high", exitDirection ? "right" : "left"));
#endif

            // Create a new internal node that will hold the new leaf.            
            var newLeaf = new Leaf(cutPoint.LongestPrefix + 1, key, value);
            var newInternal = new Internal(exitNode.NameLength, cutPoint.LongestPrefix);

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
            if (exitNode == this.Root)
            {
                // Then update the root
                this.Root = newInternal;
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
                UpdateRightJumpsAfterInsertion(newInternal, exitNode, isRightChild, newLeaf, stack);
            else
                UpdateLeftJumpsAfterInsertion(newInternal, exitNode, isRightChild, newLeaf, stack);

            unsafe
            {
                // If the cut point was low and the exit node internal
                if (isCutLow && exitNodeAsInternal != null)
                {
                    uint hash = CalculateHashForBits(searchKey, hashState, exitNodeAsInternal.GetHandleLength(this));
                    ReplaceExistingNodeInTable(hash, exitNodeAsInternal, newInternal);
#if DETAILED_DEBUG
                    {
                        int prefixLength = exitNodeHandleLength / BitVector.BitsPerByte;
                        if (exitNodeHandleLength % BitVector.BitsPerByte != 0)
                            prefixLength++;

                        Console.WriteLine(string.Format("\tMap.Replace -> prefix: '{0}', bytes: {1} ", searchKey.SubVector(0, exitNodeHandleLength).ToDebugString(), prefixLength));
                    }                                       
#endif
                    exitNodeAsInternal.NameLength = cutPoint.LongestPrefix + 1;

                    hash = CalculateHashForBits(exitNodeAsInternal.Name(this), hashState, exitNodeAsInternal.GetHandleLength(this));
                    InsertNodeInTable(hash, exitNodeAsInternal);

                    //  We update the jumps for the exit node.                
                    UpdateJumps(exitNodeAsInternal);
#if DETAILED_DEBUG
                    {
                        // DEBUG
                        int handleLength = exitNodeAsInternal.GetHandleLength(this);
                        int prefixLength = handleLength / BitVector.BitsPerByte;
                        if (handleLength % BitVector.BitsPerByte != 0)
                            prefixLength++;

                        Console.WriteLine(string.Format("\tMap.Add-low -> prefix: '{0}', bytes: {1} ", exitNodeAsInternal.Name(this).SubVector(0, handleLength).ToDebugString(), prefixLength));
                    }                    
#endif
                }
                else
                {
                    //  We add the internal node to the jump table.                
                    exitNode.NameLength = cutPoint.LongestPrefix + 1;

                    uint hash = CalculateHashForBits(searchKey, hashState, newInternal.GetHandleLength(this));                  

                    InsertNodeInTable(hash, newInternal);
#if DETAILED_DEBUG
                    {
                        // DEBUG
                        int handleLength = newInternal.GetHandleLength(this);
                        int prefixLength = handleLength / BitVector.BitsPerByte;
                        if (handleLength % BitVector.BitsPerByte != 0)
                            prefixLength++;

                        Console.WriteLine(string.Format("\tMap.Add -> prefix: '{0}', bytes: {1} ", newInternal.Handle(this).ToDebugString(), prefixLength));
                    }
#endif
                }
            }

            // Link the new leaf with it's predecessor and successor.
            if ( exitDirection )
                AddAfter(exitNode.GetRightLeaf(), newLeaf);
            else
                AddBefore(exitNode.GetLeftLeaf(), newLeaf);

            size++;

#if DETAILED_DEBUG
            Console.WriteLine(DumpNodesTable());
#endif

            return true;
        }

        private static uint CalculateHashForBits(BitVector vector, Hashing.Iterative.XXHash32Block state, int length = int.MaxValue)
        {
            length = Math.Min(vector.Count, length); // Ensure we use the proper value.

            int bytes = length / BitVector.BitsPerWord;
            int remaining = length % BitVector.BitsPerWord;

            ulong remainingWord = 0;
            if (remaining != 0)
            {
                remainingWord = vector.GetWord(bytes); // Zero addressing ensures we get the next byte.
                remainingWord >>= BitVector.BitsPerWord - remaining;
            }

            unsafe
            {
                fixed (ulong* bitsPtr = vector.Bits)
                {
                    uint hash = Hashing.Iterative.XXHash32.Calculate((byte*)bitsPtr, bytes, state);
#if DETAILED_DEBUG
                    Console.WriteLine(string.Format("\tHash -> Prefix: {0}, Remaining: {2}, Bits({1})", hash, remaining, remainingWord));
#endif
                    uint upper = (uint)(remainingWord >> 32);
                    uint bottom = (uint)remainingWord;

                    return Hashing.CombineInline(hash, Hashing.CombineInline(Hashing.CombineInline(upper, bottom), (uint)length));
                }
            }
        }

        private void InsertNodeInTable(uint hash, Internal node)
        {
            List<Internal> values;
            if (!this.NodesTable.TryGetValue(hash, out values))
            {
                values = new List<Internal>();
                this.NodesTable[hash] = values;
            }

            if (!values.Contains(node))
            {
#if DETAILED_DEBUG
                Console.WriteLine(string.Format("\tMap.Add -> Node:{0}, Hash:{1}", node.ToDebugString(this), hash));
#endif
                values.Add(node);
            }
                
        }


        private void ReplaceExistingNodeInTable(uint hash, Internal oldNode, Internal newNode)
        {
            List<Internal> values;
            if (!this.NodesTable.TryGetValue(hash, out values))
            {
                values = new List<Internal>();
                this.NodesTable[hash] = values;
            }

            var removed = values.Remove(oldNode);
            values.Add(newNode);
#if DETAILED_DEBUG
            Console.WriteLine(string.Format("\tMap.Replace -> Add:{0}, ToRemove: {1}, Found: {2}, Hash:{3}", newNode.ToDebugString(this), oldNode.ToDebugString(this), removed, hash));
#endif
        }

        private void UpdateJumps(Internal node)
        {
            int jumpLength = node.GetJumpLength(this);

            Node jumpNode;
            for (jumpNode = node.Left; jumpNode is Internal && jumpLength > ((Internal)jumpNode).ExtentLength; )
                jumpNode = ((Internal)jumpNode).JumpLeftPtr;

            node.JumpLeftPtr = jumpNode;

            for (jumpNode = node.Right; jumpNode is Internal && jumpLength > ((Internal)jumpNode).ExtentLength; )
                jumpNode = ((Internal)jumpNode).JumpRightPtr;

            node.JumpRightPtr = jumpNode;
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
                var leaf = (Leaf)this.Root;
                if (leaf.Key.Equals(key))
                    return false;

                RemoveLeaf(leaf);

                // We remove the root.                
                this.Root = null;
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

            return SuccessorInternal(key).Key;
        }

        public TKey Predecessor(TKey key)
        {
            if (size == 0)
                throw new KeyNotFoundException();

            return PredecessorInternal(key).Key;
        }

        public TKey FirstKey()
        {
            if (size == 0)
                throw new KeyNotFoundException();

            // Returns the head next key
            return this.Head.Next.Key;
        }

        public TKey LastKey()
        {
            if (size == 0)
                throw new KeyNotFoundException();

            // Returns the head next key
            return this.Tail.Previous.Key;
        }

        public void Clear()
        {
            this.size = 0;
            this.Root = null;
            this.NodesTable.Clear();
            this.Head.Next = this.Tail;
            this.Tail.Previous = this.Head;
        }

        public TKey SuccessorOrDefault(TKey key)
        {
            // The set is empty, there is no successor to any key.
            if (size == 0)
                return default(TKey);

            return SuccessorInternal(key).Key;
        }

        public TKey PredecessorOrDefault(TKey key)
        {
            // The set is empty, there is no predecessor to any key.
            if (size == 0)
                return default(TKey);

            return PredecessorInternal(key).Key;
        }

        public TKey FirstKeyOrDefault()
        {
            if (size == 0)
                return default(TKey);

            // Returns the head next key
            return this.Head.Next.Key;
        }

        public TKey LastKeyOrDefault()
        {
            if (size == 0)
                return default(TKey);

            // Returns the head next key
            return this.Tail.Previous.Key;
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
        internal Leaf PredecessorInternal(TKey key)
        {            
            // x− = max{y ∈ S | y < x} (the predecessor of x in S) - Page 160 of [1]

            // We look for the exit node for the key
            var exitFound = FindExitNode(key);
            var exitNode = exitFound.Exit;

            // We compare the key with the exit node extent.
            int dummy;
            if (exitNode.Extent(this).CompareToInline(exitFound.SearchKey, out dummy) < 0)
            {
                // If the key is greater than the extent, we exit to the right leaf.
                return exitNode.GetRightLeaf();
            }
            else
            {
                // If the key is smaller than the extent, we exit to the left leaf and get the previous leaf.
                return exitNode.GetLeftLeaf().Previous;
            }            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Leaf SuccessorInternal(TKey key)
        {
            // x+ = min{y ∈ S | y ≥ x} (the successor of x in S) - Page 160 of [1]

            // We look for the exit node for the key
            var exitFound = FindExitNode(key);
            var exitNode = exitFound.Exit;

            // We compare the key with the exit node extent.
            int dummy;
            if (exitFound.SearchKey.CompareToInline(exitNode.Extent(this), out dummy) <= 0)
            {
                // If the key is smaller than the extent, we exit to the left leaf.
                return exitNode.GetLeftLeaf();
            }
            else
            {
                // If the key is greater than the extent, we exit to the right leaf and get the next.
                return exitNode.GetRightLeaf().Next;
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

        private void UpdateRightJumpsAfterInsertion(Internal insertedNode, Node exitNode, bool isRightChild, Leaf insertedLeaf, Stack<Internal> stack)
        {
#if DETAILED_DEBUG
            Console.WriteLine(string.Format("UpdateRightJumpsAfterInsertion({0}, {1}, {2}, {3}, {4})", insertedNode.ToDebugString(this), exitNode.ToDebugString(this), isRightChild, insertedLeaf.ToDebugString(this), DumpStack(stack)));
#endif
            if ( ! isRightChild )
            {
                // Not all the jump pointers of 2-fat ancestors need to be updated: actually, we
                // need to update only pointers to nodes that are left descendant of β.

                while ( stack.Count != 0 )
                {
                    var toFix = stack.Pop();                    
                    if (toFix.JumpLeftPtr != exitNode)
                        break;

                    int jumpLength = toFix.GetJumpLength(this);
                    if ( jumpLength < insertedLeaf.NameLength )
                        toFix.JumpLeftPtr = insertedNode;
                }
            }
            else
            {
                // Not all the jump pointers of 2-fat ancestors need to be updated: actually, we
                // need to update only pointers to nodes that are right descendant of β.

                while( stack.Count != 0 )
                {
                    var toFix = stack.Peek();
                    int jumpLength = toFix.GetJumpLength(this);
                    if (toFix.JumpRightPtr != exitNode || jumpLength >= insertedLeaf.NameLength)
                        break;

                    toFix.JumpRightPtr = insertedNode;
                    stack.Pop();
                }

                while( stack.Count != 0 )
                {
                    var toFix = stack.Pop();
                    int jumpLength = toFix.GetJumpLength(this);

                    while (exitNode is Internal && toFix.JumpRightPtr != exitNode)
                        exitNode = ((Internal)exitNode).JumpRightPtr;

                    // As soon as we cannot find a matching descendant, we can stop updating
                    if (toFix.JumpRightPtr != exitNode)
                        return;

                    toFix.JumpRightPtr = insertedLeaf;
                }
            }
        }

        private void UpdateLeftJumpsAfterInsertion(Internal insertedNode, Node exitNode, bool isRightChild, Leaf insertedLeaf, Stack<Internal> stack)
        {
#if DETAILED_DEBUG
            Console.WriteLine(string.Format("UpdateLeftJumpsAfterInsertion({0}, {1}, {2}, {3}, {4})", insertedNode.ToDebugString(this), exitNode.ToDebugString(this), isRightChild, insertedLeaf.ToDebugString(this), DumpStack(stack)));
#endif
            // See: Algorithm 2 of [1]

            if ( isRightChild )
            {
                // Not all the jump pointers of 2-fat ancestors need to be updated: actually, we
                // need to update only pointers to nodes that are right descendant of β.

                while ( stack.Count != 0 )
                {
                    var toFix = stack.Pop();
                    if (toFix.JumpRightPtr != exitNode)
                        break;

                    int jumpLength = toFix.GetJumpLength(this);
                    if (jumpLength < insertedLeaf.NameLength)
                        toFix.JumpRightPtr = insertedNode;
                }
            }
            else
            {
                // Not all the jump pointers of 2-fat ancestors need to be updated: actually, we
                // need to update only pointers to nodes that are left descendant of β.

                while ( stack.Count != 0 )
                {
                    var toFix = stack.Peek();
                    int jumpLength = toFix.GetJumpLength(this);
                    if (toFix.JumpLeftPtr != exitNode || jumpLength >= insertedLeaf.NameLength)
                        break;

                    toFix.JumpLeftPtr = insertedNode;
                    stack.Pop();
                }

                while ( stack.Count != 0 )
                {
                    var toFix = stack.Pop();
                    while (exitNode is Internal && toFix.JumpLeftPtr != exitNode)
                        exitNode = ((Internal)exitNode).JumpLeftPtr;

                    // As soon as we cannot find a matching descendant, we can stop updating
                    if (toFix.JumpLeftPtr != exitNode)
                        return;
                    
                    toFix.JumpLeftPtr = insertedLeaf;
                }
            }
        }


        internal CutPoint FindParentExitNode(BitVector searchKey, Stack<Internal> stack = null)
        {
            Contract.Requires(size != 0);
#if DETAILED_DEBUG
            Console.WriteLine(string.Format("FindParentExitNode({0})", searchKey.ToBinaryString()));
#endif
            // If there is only a single element, then the exit point is the root.
            if (size == 1)
                return new CutPoint(searchKey.LongestCommonPrefixLength(this.Root.Extent(this)), null, Root, searchKey);
            
            if (stack == null)
                stack = new Stack<Internal>();

            int length = searchKey.Count;
            var state = Hashing.Iterative.XXHash32.Preprocess(searchKey.Bits);

            // Find parex(key), exit(key) or fail spectacularly (with very low probability). 
            Internal parexOrExitNode = FatBinarySearch(searchKey, state, stack, -1, length);

            // Check if the node is either the parex(key) and/or exit(key). 
            Node candidateNode;
            if (parexOrExitNode.ExtentLength < length && searchKey[parexOrExitNode.ExtentLength])
                candidateNode = parexOrExitNode.Right;
            else
                candidateNode = parexOrExitNode.Left;

            int lcpLength = searchKey.LongestCommonPrefixLength(candidateNode.Extent(this));            

            // Fat Binary Search just worked with high probability and gave use the parex(key) node. 
            if (candidateNode.IsExitNodeOf(this, searchKey.Count, lcpLength))
                return new CutPoint(lcpLength, parexOrExitNode, candidateNode, searchKey);

            // We need to find the length of the longest common prefix between the key and the extent of the parex(key).
            lcpLength = Math.Min(parexOrExitNode.ExtentLength, lcpLength);

            Contract.Assert(lcpLength == searchKey.LongestCommonPrefixLength(parexOrExitNode.Extent(this)));

            int startPoint;
            if ( parexOrExitNode.IsExitNodeOf(this, length, lcpLength) )
            {
                // We have the correct exit node, we then must pop it and probably restart the search to find the parent.
                stack.Pop();

                // If the exit node is the root, there is obviously no parent to be found.
                if (parexOrExitNode == this.Root)
                    return new CutPoint(lcpLength, null, parexOrExitNode, searchKey);

                startPoint = stack.Peek().ExtentLength;
                if (startPoint == parexOrExitNode.NameLength - 1)
                    return new CutPoint(lcpLength, stack.Peek(), parexOrExitNode, searchKey);

                // Find parex(key) or fail spectacularly (with very low probability). 
                int stackSize = stack.Count;

                Internal parexNode = FatBinarySearch(searchKey, state, stack, startPoint, parexOrExitNode.NameLength);
                if (parexNode.Left == parexOrExitNode || parexNode.Right == parexOrExitNode)
                    return new CutPoint(lcpLength, parexNode, parexOrExitNode, searchKey);

                // It seems we just failed and found an unrelated node, we should restart in exact mode and also clear the stack of what we added during the last search.
                while (stack.Count > stackSize)
                    stack.Pop();
                
                parexNode = FatBinarySearchExact(searchKey, state, stack, startPoint, parexOrExitNode.NameLength);

                return new CutPoint(lcpLength, parexNode, parexOrExitNode, searchKey);
            }
            
            // The search process failed with very low probability.
            stack.Clear();
            parexOrExitNode = FatBinarySearchExact(searchKey, state, stack, -1, length);

            if (parexOrExitNode.ExtentLength < length && searchKey[parexOrExitNode.ExtentLength])
                candidateNode = parexOrExitNode.Right;
            else
                candidateNode = parexOrExitNode.Left;

            lcpLength = searchKey.LongestCommonPrefixLength(candidateNode.Extent(this));

            // Fat Binary Search just worked with high probability and gave use the parex(key) node. 
            if (candidateNode.IsExitNodeOf(this, searchKey.Count, lcpLength))
                return new CutPoint(lcpLength, parexOrExitNode, candidateNode, searchKey);

            stack.Pop();

            // If the exit node is the root, there is obviously no parent to be found.
            if (parexOrExitNode == this.Root)
                return new CutPoint(lcpLength, null, parexOrExitNode, searchKey);

            startPoint = stack.Peek().ExtentLength;
            if (startPoint == parexOrExitNode.NameLength - 1)
                return new CutPoint(lcpLength, stack.Peek(), parexOrExitNode, searchKey);

            Internal parentNode = FatBinarySearchExact(searchKey, state, stack, startPoint, parexOrExitNode.NameLength);
            return new CutPoint(lcpLength, parentNode, parexOrExitNode, searchKey);
        }

        internal ExitNode FindExitNode(TKey key, Stack<Internal> stack = null)
        {
            Contract.Requires(size != 0);

            // We prepare the signature to compute incrementally. 
            BitVector searchKey = binarizeFunc(key);          

            if (size == 1)
                return new ExitNode(searchKey.LongestCommonPrefixLength(this.Root.Extent(this)), this.Root, searchKey);

            if (stack == null)
                stack = new Stack<Internal>();

            var state = Hashing.Iterative.XXHash32.Preprocess(searchKey.Bits);

            // Find parex(key), exit(key) or fail spectacularly (with very low probability). 
            Internal parexOrExitNode = FatBinarySearch(searchKey, state, stack, -1, searchKey.Count);

            // Check if the node is either the parex(key) and/or exit(key). 
            Node candidateNode;
            if (parexOrExitNode.ExtentLength < searchKey.Count && searchKey[parexOrExitNode.ExtentLength])
                candidateNode = parexOrExitNode.Right;
            else
                candidateNode = parexOrExitNode.Left;

            int lcpLength = searchKey.LongestCommonPrefixLength(candidateNode.Extent(this));

            // Fat Binary Search just worked with high probability and gave use the parex(key) node. 
            if (candidateNode.IsExitNodeOf(this, searchKey.Count, lcpLength))
                return new ExitNode(lcpLength, candidateNode, searchKey);

            lcpLength = Math.Min( parexOrExitNode.ExtentLength, lcpLength );
            if (parexOrExitNode.IsExitNodeOf(this, searchKey.Count, lcpLength))
                return new ExitNode(lcpLength, parexOrExitNode, searchKey);

            // With very low priority we screw up and therefore we start again but without skipping anything. 
            parexOrExitNode = FatBinarySearchExact(searchKey, state, stack, -1, searchKey.Count);
            if (parexOrExitNode.Extent(this).IsProperPrefix(searchKey))
            {
                if (parexOrExitNode.ExtentLength < searchKey.Count && searchKey[parexOrExitNode.ExtentLength])
                    candidateNode = parexOrExitNode.Right;
                else
                    candidateNode = parexOrExitNode.Left;
            }
            else
            {
                candidateNode = parexOrExitNode;
            }

            return new ExitNode(searchKey.LongestCommonPrefixLength(candidateNode.Extent(this)), candidateNode, searchKey);
        }

        private unsafe Internal FatBinarySearch(BitVector searchKey, Hashing.Iterative.XXHash32Block state, Stack<Internal> stack, int startBit, int endBit)
        {
            Contract.Requires(searchKey != null);
            Contract.Requires(state != null);
            Contract.Requires(stack != null);
            Contract.Requires(startBit < endBit - 1);
#if DETAILED_DEBUG
            Console.WriteLine(string.Format("FatBinarySearch({0},{1},({2}..{3})", searchKey.ToDebugString(), DumpStack(stack), startBit, endBit));
#endif
            endBit--;

            Internal top = stack.Count != 0 ? stack.Peek() : null;

            if (startBit == -1)
            {
                Contract.Assert(this.Root is Internal);

                top = (Internal)this.Root;
                stack.Push(top);
                startBit = top.ExtentLength;
            }

            uint checkMask = (uint)(-1 << Bits.CeilLog2(endBit - startBit));
            while (endBit - startBit > 0)
            {
                Contract.Assert(checkMask != 0);
#if DETAILED_DEBUG
                Console.WriteLine(string.Format("({0}..{1})", startBit, endBit + 1));
#endif
                int current = endBit & (int)checkMask;
                if ((startBit & checkMask) != current)
                {
#if DETAILED_DEBUG
                    Console.WriteLine(string.Format("Inquiring with key {0} ({1})", searchKey.SubVector(0, current).ToBinaryString(), current));
#endif
                    // We calculate the hash up to the word it makes sense. 
                    uint hash = CalculateHashForBits(searchKey, state, current);
#if DETAILED_DEBUG
                    {
                        int prefixLength = current / BitVector.BitsPerByte;
                        if (current % BitVector.BitsPerByte != 0)
                            prefixLength++;

                        Console.WriteLine(string.Format("\tMap.Locate -> key:{0}, bytes:{1}, hash:{2}", searchKey.SubVector(0, current).ToBinaryString(), prefixLength, hash));                
                    }
#endif                    
                    bool found = false;

                    List<Internal> items;
                    if (NodesTable.TryGetValue(hash, out items))
                    {
                        // If we only have a single match there only check we need is the item.ExtentLenght < current
                        // However, if there are multiple matches, then we should also ensure that the Handle has the same size and 
                        // that the reference pointer to the leaf share the prefix up to the current query size. 
                        bool isSingle = items.Count == 1;

                        foreach( var item in items )
                        {
                            if ( item.ExtentLength >= current )
                            {
                                if (!isSingle && (item.GetHandleLength(this) != current || item.ReferencePtr.Name(this).IsPrefix(searchKey, current))) 
                                                                                       /// searchKey.CompareTo(item.ReferencePtr.Name(this), current) != 0 ))
                                    continue;
#if DETAILED_DEBUG
                                Console.WriteLine("Found extent of length " + item.ExtentLength + " with GetExtentLength of " + item.GetExtentLength(this));
#endif
                                // Add it to the stack, update search and continue
                                top = item;
                                if (stack != null)
                                    stack.Push(top);

                                startBit = item.ExtentLength;
                                found = true;

                                break;
                            }
                        }
                    }

                    if (found == false)
                    {
#if DETAILED_DEBUG
                        Console.WriteLine("Missing");
#endif
                        endBit = current - 1;
                    }
                }

                checkMask >>= 1;
            }

#if DETAILED_DEBUG
            Console.WriteLine(string.Format("Final interval: ({0}..{1}); Top: {2}; Stack: {3}", startBit, endBit + 1, top.ToDebugString(this), DumpStack(stack)));
#endif
            return top;
        }

        private unsafe Internal FatBinarySearchExact(BitVector searchKey, Hashing.Iterative.XXHash32Block state, Stack<Internal> stack, int startBit, int endBit)
        {
            Contract.Requires(searchKey != null);
            Contract.Requires(state != null);
            Contract.Requires(stack != null);
            Contract.Requires(startBit < endBit - 1);
#if DETAILED_DEBUG
            Console.WriteLine(string.Format("FatBinarySearchExact({0},{1},({2}..{3})", searchKey.ToDebugString(), DumpStack(stack), startBit, endBit));
#endif
            endBit--;

            Internal top = stack.Count != 0 ? stack.Peek() : null;
           
            if ( startBit == -1 )
            {
                Contract.Assert(this.Root is Internal);

                top = (Internal) this.Root;
                stack.Push(top);
                startBit = top.ExtentLength;
            }

            int checkMask = -1 << Bits.CeilLog2(endBit - startBit);
            while (endBit - startBit > 0)
            {
                Contract.Assert(checkMask != 0);
#if DETAILED_DEBUG
                Console.WriteLine(string.Format("({0}..{1})", startBit, endBit + 1));
#endif
                int current = endBit & checkMask;
                if ((startBit & checkMask) != current)
                {
#if DETAILED_DEBUG
                    Console.WriteLine(string.Format("Inquiring with key {0} ({1})", searchKey.SubVector(0, current).ToBinaryString(), current));
#endif
                    // We calculate the hash up to the word it makes sense. 
                    uint hash = CalculateHashForBits(searchKey, state, current);
#if DETAILED_DEBUG
                    {
                        int prefixLength = current / BitVector.BitsPerByte;
                        if (current % BitVector.BitsPerByte != 0)
                            prefixLength++;

                        Console.WriteLine(string.Format("\tMap.Locate -> key:{0}, bytes:{1}, hash:{2}", searchKey.SubVector(0, current).ToBinaryString(), prefixLength, hash));   
                    }
#endif

                    bool found = false;                    

                    List<Internal> items;
                    if (NodesTable.TryGetValue(hash, out items))
                    {
                        // Try all items to find an exact match
                        foreach (var item in items)
                        {
                            // If the node matches the handle length, the reference ptr name is equal and it is not a false positive
                            if (item.GetHandleLength(this) == current &&
                                 searchKey.CompareTo(item.ReferencePtr.Name(this), current) == 0 &&
                                // Make sure it is not a false positive
                                 item.ExtentLength < current)
                            {
#if DETAILED_DEBUG
                                Console.WriteLine("Found extent of length " + item.ExtentLength);
#endif
                                // Add it to the stack, update search and continue
                                top = item;
                                if (stack != null)
                                    stack.Push(top);

                                startBit = item.ExtentLength;
                                found = true;
                                break;
                            }
                        }
                    }

                    // We haven't found an exact match. 
                    if (!found)
                    {
#if DETAILED_DEBUG
                        Console.WriteLine("Missing");
#endif
                        endBit = current - 1;
                    }
                }

                checkMask >>= 1;
            }
#if DETAILED_DEBUG
            Console.WriteLine(string.Format("Final interval: ({0}..{1}); Top: {2}; Stack: {3}", startBit, endBit + 1, top.ToDebugString(this), DumpStack(stack)));
#endif
            return top;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int TwoFattest( int a, int b)
        {
            return -1 << Bits.MostSignificantBit(a ^ b) & b;
        }

        private string DumpNodesTable()
        {
            var builder = new StringBuilder();

            bool first = true;
            builder.Append("After Insertion. NodesTable: {");
            foreach (var item in this.NodesTable)
            {
                var node = item.Value.First();

                if (!first )
                    builder.Append(", ");

                builder.Append(node.Handle(this).ToDebugString())
                       .Append(" => ")
                       .Append(node.ToDebugString(this));
                
                first = false;
            }
            builder.Append("} Root: ")
                   .Append( this.Root.ToDebugString(this) );

            return builder.ToString();
        }

        private string DumpStack<T>(Stack<T> stack) where T : Node
        {
            var builder = new StringBuilder();
            builder.Append("[");

            bool first = true;
            foreach ( var node in stack )
            {
                if ( !first )
                    builder.Append(", ");
                
                builder.Append(node.ToDebugString(this));

                first = false;
            }

            builder.Append("] ");

            return builder.ToString();
        }

    }
}

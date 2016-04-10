using System;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Voron.Data.Compact
{
    public unsafe partial class PrefixTree
    {
        public enum NodeType : byte
        {
            Uninitialized = 0,
            Internal = 1,
            Leaf = 2,
            Tombstone = 3
        }

        [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 48)]
        public struct Node
        {
            [FieldOffset(0)]
            public NodeType Type;

            [FieldOffset(2)]
            public short NameLength;

            /// <summary>
            /// In the leaf it is the pointer to the nearest internal cut node. In the internal node, it is a pointer to any leaf 
            /// in the subtree, as all leaves will share the same key prefix.
            /// </summary>
            [FieldOffset(4)]
            public long ReferencePtr;

            [FieldOffset(12)]
            public fixed byte Padding[36]; // to 48 bytes

            public bool IsLeaf => Type == NodeType.Leaf;
            public bool IsInternal => Type == NodeType.Internal;
            public bool IsTombstone => Type == NodeType.Tombstone;
            public bool IsUninitialized => !IsLeaf && !IsInternal && !IsTombstone;

            public static bool IsTombstonePtr(long ptr)
            {
                ulong uPtr = (ulong)ptr;
                return uPtr > unchecked((ulong)Constants.TombstoneNodeName) && uPtr != unchecked((ulong)Constants.InvalidNodeName);
            }

            public static bool IsValidPtr(long ptr)
            {
                ulong uPtr = (ulong)ptr;
                return uPtr < unchecked((ulong)Constants.TombstoneNodeName);
            }
        }

        /// <summary>
        /// Every internal node contains a pointer to its two children, the extremes ia and ja of its skip interval,
        /// its own extent ea and two additional jump pointers J- and J+. Page 163 of [1].
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 48)]
        public struct Internal
        {
            [FieldOffset(0)]
            public NodeType Type;

            [FieldOffset(2)]
            public short NameLength;

            /// <summary>
            /// In the leaf it is the pointer to the nearest internal cut node. In the internal node, it is a pointer to any leaf 
            /// in the subtree, as all leaves will share the same key prefix.
            /// </summary>
            [FieldOffset(4)]
            public long ReferencePtr;

            /// <summary>
            /// The right subtrie.
            /// </summary>
            // TODO: Given that we are using an implicit representation is this necessary?
            //       Wouldnt be the same naming the current node and save 4 bytes per node?
            [FieldOffset(12)]
            public long RightPtr;

            /// <summary>
            /// The left subtrie.
            /// </summary>
            // TODO: Given that we are using an implicit representation is this necessary?
            //       Wouldnt be the same naming the current node and save 4 bytes per node?
            [FieldOffset(20)]
            public long LeftPtr;

            /// <summary>
            /// The downward right jump pointer.
            /// </summary>
            [FieldOffset(28)]
            public long JumpRightPtr;

            /// <summary>
            /// The downward left jump pointer.
            /// </summary>
            [FieldOffset(36)]
            public long JumpLeftPtr;

            [FieldOffset(44)]
            public short ExtentLength;

            [FieldOffset(46)]
            public fixed byte Padding[2]; // to 48 bytes

            public Internal(short nameLength = 0, short extentLength = 0)
            {
                this.Type = NodeType.Internal;
                this.NameLength = nameLength;
                this.ExtentLength = extentLength;

                this.ReferencePtr = Constants.InvalidNodeName;
                this.RightPtr = Constants.InvalidNodeName;
                this.JumpLeftPtr = Constants.InvalidNodeName;
                this.JumpRightPtr = Constants.InvalidNodeName;
                this.LeftPtr = Constants.InvalidNodeName;
            }

            public bool IsLeaf => Type == NodeType.Leaf;
            public bool IsInternal => Type == NodeType.Internal;
            public bool IsTombstone => Type == NodeType.Tombstone;
            public bool IsUninitialized => !IsLeaf && !IsInternal && !IsTombstone;

            internal void Initialize(short nameLength, short extentLength)
            {
                this.Type = NodeType.Internal;
                this.NameLength = nameLength;
                this.ExtentLength = extentLength;

                this.ReferencePtr = Constants.InvalidNodeName;
                this.RightPtr = Constants.InvalidNodeName;
                this.JumpLeftPtr = Constants.InvalidNodeName;
                this.JumpRightPtr = Constants.InvalidNodeName;
                this.LeftPtr = Constants.InvalidNodeName;
            }
        }


        /// <summary>
        /// Leaves are organized in a double linked list: each leaf, besides a pointer to the corresponding string of S, 
        /// stores two pointers to the next/previous leaf in lexicographic order. Page 163 of [1].
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 48)]
        public struct Leaf
        {
            [FieldOffset(0)]
            public NodeType Type;

            [FieldOffset(2)]
            public short NameLength;

            /// <summary>
            /// In the leaf it is the pointer to the nearest internal cut node. In the internal node, it is a pointer to any leaf 
            /// in the subtree, as all leaves will share the same key prefix.
            /// </summary>
            [FieldOffset(4)]
            public long ReferencePtr;

            /// <summary>
            /// The previous leaf in the double linked list referred in page 163 of [1].
            /// </summary>
            [FieldOffset(16)]
            public long PreviousPtr;

            /// <summary>
            /// The public leaf in the double linked list referred in page 163 of [1].
            /// </summary>
            [FieldOffset(24)]
            public long NextPtr;

            /// <summary>
            /// The stored original value passed.
            /// </summary>
            [FieldOffset(32)]
            public long DataPtr;

            /// <summary>
            /// The stored key size to avoid having to go getting the data.
            /// </summary>
            [FieldOffset(40)]
            public ushort KeySize;

            [FieldOffset(42)]
            public fixed byte Padding[6]; // to 48 bytes

            public Leaf(long previousPtr = Constants.InvalidNodeName, long nextPtr = Constants.InvalidNodeName)
            {
                this.Type = NodeType.Leaf;
                this.NameLength = 0;

                this.NextPtr = nextPtr;
                this.PreviousPtr = previousPtr;
                this.ReferencePtr = Constants.InvalidNodeName;
                this.DataPtr = Constants.InvalidNodeName;
                this.KeySize = 0;
            }

            public void Initialize(short nameLength, long previousPtr = Constants.InvalidNodeName, long nextPtr = Constants.InvalidNodeName)
            {
                this.Type = NodeType.Leaf;
                this.NameLength = nameLength;

                this.NextPtr = nextPtr;
                this.PreviousPtr = previousPtr;
                this.ReferencePtr = Constants.InvalidNodeName;
                this.DataPtr = Constants.InvalidNodeName;
                this.KeySize = 0;
            }

            public bool IsLeaf => Type == NodeType.Leaf;
            public bool IsInternal => Type == NodeType.Internal;
            public bool IsTombstone => Type == NodeType.Tombstone;
            public bool IsUninitialized => !IsLeaf && !IsInternal && !IsTombstone;
        }
    }
}

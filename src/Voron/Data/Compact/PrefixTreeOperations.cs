using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using static Voron.Data.Compact.PrefixTree;

namespace Voron.Data.Compact
{
    public unsafe static class PrefixTreeOperations
    {
        /// <summary>
        /// The name of a node, is the string deprived of the string stored at it. Page 163 of [1]
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitVector Name(this PrefixTree tree, Leaf* @this)
        {
            Debug.Assert(@this->IsLeaf);

            Slice key = tree.ReadKey(@this->DataPtr);
            return key.ToBitVector();
        }

        /// <summary>
        /// The name of a node, is the string deprived of the string stored at it. Page 163 of [1]
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitVector Name(this PrefixTree tree, Node* @this)
        {
            if (@this->IsInternal)
            {
                var refNode = tree.DirectRead(((Internal*)@this)->ReferencePtr);
                return tree.Name(refNode);
            }
            else
            {                
                return tree.Name((Leaf*)@this);
            }
        }

        /// <summary>
        /// The handle of a node is the prefix of the name whose length is 2-fattest number in the skip interval of it. If the
        /// skip interval is empty (which can only happen at the root) we define the handle to be the empty string/vector.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitVector Handle(this PrefixTree tree, Node* @this)
        {
            // This cannot happen. We will never call Handle() in a single item tree where the root is a leaf. 
            Debug.Assert(@this->ReferencePtr != Constants.InvalidNodeName); 

            var refNode = tree.DirectRead(@this->ReferencePtr);
            if (@this->IsInternal)
            {
                int handleLength = tree.GetHandleLength(@this);
                return tree.Name(refNode).SubVector(0, handleLength);
            }
            else
            {
                return tree.Name(refNode).SubVector(0, tree.GetHandleLength(@this));
            }
        }

        /// <summary>
        /// The extent of a node, is the longest common prefix of the strings represented by the leaves that are descendants of it.
        /// </summary>  
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static BitVector Extent(this PrefixTree tree, Node* @this)
        {          
            if (@this->IsInternal)
            {
                var refNode = tree.DirectRead(@this->ReferencePtr);
                var refName = tree.Name(refNode);
                return refName.SubVector(0, ((Internal*)@this)->ExtentLength);
            }
            else
            {
                var leaf = (Leaf*)@this;
                return tree.ReadKey(leaf->DataPtr).ToBitVector();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetHandleLength(this PrefixTree tree, Node* @this)
        {
            return TwoFattest(@this->NameLength - 1, tree.GetExtentLength(@this));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetHandleLength(this PrefixTree tree, Internal* @this)
        {
            return TwoFattest(@this->NameLength - 1, @this->ExtentLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetExtentLength(this PrefixTree tree, Internal* @this)
        {
            return @this->ExtentLength;  
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetExtentLength(this PrefixTree tree, Node* @this)
        {
            if (@this->IsInternal)
            {
                return ((Internal*)@this)->ExtentLength;
            }
            else
            {                
                return ((Leaf*)@this)->KeySize;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetJumpLength(this PrefixTree tree, Internal* @this)
        {
            int handleLength = tree.GetHandleLength(@this);
            if (handleLength == 0)
                return int.MaxValue;

            return handleLength + (handleLength & -handleLength);
        }

        /// <summary>
        /// There are two cases. We say that x cuts high if the cutpoint is strictly smaller than |handle(exit(x))|, cuts low otherwise. Page 165 of [1]
        /// </summary>
        /// <remarks>Only when the cut is low, the handle(exit(x)) is a prefix of x.</remarks>        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsCutLow(this PrefixTree owner, Node* node, long prefix)
        {
            // Theorem 3: Page 165 of [1]
            var handleLength = owner.GetHandleLength(node);
            return prefix >= handleLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsExitNodeOf(this PrefixTree tree, Node* @this, int length, int lcpLength)
        {
            return @this->NameLength <= lcpLength && (lcpLength < tree.GetExtentLength(@this) || lcpLength == length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsExitNodeOf(this PrefixTree tree, Internal* @this, int length, int lcpLength)
        {
            return @this->NameLength <= lcpLength && (lcpLength < tree.GetExtentLength(@this) || lcpLength == length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long GetRightLeaf(this PrefixTree tree, long @thisName)
        {
            var @this = tree.DirectRead(@thisName);
            if (@this->IsLeaf)
                return @thisName;

            long nodeName;
            Node* node = @this;
            do
            {
                nodeName = ((Internal*)node)->JumpRightPtr;
                node = tree.DirectRead(nodeName);
            }
            while (node->IsInternal);

            return nodeName;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long GetLeftLeaf(this PrefixTree tree, long @thisName)
        {
            var @this = tree.DirectRead(@thisName);
            if (@this->IsLeaf)
                return @thisName;

            long nodeName;
            Node* node = @this;
            do
            {
                nodeName = ((Internal*)node)->JumpLeftPtr;
                node = tree.DirectRead(nodeName);
            }
            while (node->IsInternal);

            return nodeName;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Intersects(this PrefixTree tree, Node* @this, int x)
        {
            if (@this->IsInternal)
                return x >= @this->NameLength && x <= ((Internal*)@this)->ExtentLength;

            return x >= @this->NameLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int TwoFattest(int a, int b)
        {
            return -1 << Bits.MostSignificantBit(a ^ b) & b;
        }

        public static string ToDebugString(this PrefixTree tree, Node* @this)
        {
            Leaf* leaf;
            if (@this->IsInternal)
            {
                var referenceName = @this->ReferencePtr;
                leaf = (Leaf*)tree.DirectRead(referenceName);
                Debug.Assert(leaf->IsLeaf);
            }
            else
            {
                leaf = (Leaf*) @this;
            }

            var key = tree.ReadKey(leaf->DataPtr);

            BitVector extent = tree.Extent(@this);
            int extentLength = tree.GetExtentLength(@this);

            string openBracket = @this->IsLeaf ? "[" : "(";
            string closeBracket = @this->IsLeaf ? "]" : ")";
            string extentBinary = extentLength > 16 ? extent.SubVector(0, 8).ToBinaryString() + "..." + extent.SubVector(extent.Count - 8, 8).ToBinaryString() : extent.ToBinaryString();
            string lenghtInformation = "[" + @this->NameLength + ".." + extentLength + "]";
            string jumpInfo = @this->IsInternal ? (tree.GetHandleLength(@this) + "->" + (tree.GetJumpLength((Internal*) @this))) : "";

            return string.Format("{0}{2}, {4}, {3}{1}", openBracket, closeBracket, extentBinary, jumpInfo, lenghtInformation);
        }
    }
}

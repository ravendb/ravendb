//#define DETAILED_DEBUG
//#define DETAILED_DEBUG_H

using Sparrow;
using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using Voron.Data.BTrees;
using Voron.Data.RawData;
using Voron.Data.Tables;
using Voron.Impl;

namespace Voron.Data.Compact
{
    /// <summary>
    /// In-Memory Dynamic Z-Fast TRIE supporting predecessor, successor using low linear additional space. 
    /// "A dynamic z-fast trie is a compacted trie endowed with two additional pointers per internal node and with a 
    /// dictionary. [...] it can be though of as an indexing structure built on a set of binary strings S. 
    /// 
    /// As described in "Dynamic Z-Fast Tries" by Belazzougui, Boldi and Vigna in String Processing and Information
    /// Retrieval. Lecture notes on Computer Science. Volume 6393, 2010, pp 159-172 [1]
    /// </summary>
    public unsafe partial class PrefixTree
    {
        private readonly static ObjectPool<Stack<long>> nodesStackPool = new ObjectPool<Stack<long>>(() => new Stack<long>());

        private readonly LowLevelTransaction _tx;
        private readonly Tree _parent;
        private readonly InternalTable _table;
        private readonly PrefixTreeRootMutableState _state;
        private readonly PrefixTreeTranslationTableMutableState _translationTable;

        public string Name { get; set; }

        public PrefixTree(LowLevelTransaction tx, Tree parent, PrefixTreeRootMutableState state, Slice treeName)
        {
            _tx = tx;
            _parent = parent;
            _state = state;
            _table = new InternalTable(this, _tx, _state);
            _translationTable = _state.TranslationTable;

            Name = treeName.ToString();
        }

        public static PrefixTree Create(LowLevelTransaction tx, Tree parent, Slice treeName, int subtreeDepth = -1)
        {
            var header = (PrefixTreeRootHeader*)parent.DirectRead(treeName);
            if (header != null)
               throw new InvalidOperationException($"Tried to create {treeName} as a prefix tree, but it is actually a { header->RootObjectType.ToString() }");

            // We know for sure that not data exists.
            header = (PrefixTreeRootHeader*)parent.DirectAdd(treeName, sizeof(PrefixTreeRootHeader));
            header->Initialize();
            header->RootNodeName = Constants.InvalidNodeName;
            header->Head = new Leaf { Type = NodeType.Tombstone, PreviousPtr = Constants.InvalidNodeName, NextPtr = Constants.TailNodeName };
            header->Tail = new Leaf { Type = NodeType.Tombstone, PreviousPtr = Constants.HeadNodeName, NextPtr = Constants.InvalidNodeName };
            header->Items = 0;

            if (subtreeDepth == -1)
                subtreeDepth = Constants.DepthPerCacheLine;

            var state = new PrefixTreeRootMutableState(tx, header);
            state.TranslationTable.Initialize(subtreeDepth);
            state.Table.Initialize();
            state.IsModified = true;

            return new PrefixTree(tx, parent, state, treeName);    
        }

        public static bool TryOpen( LowLevelTransaction tx, Tree parent, Slice treeName, out PrefixTree tree )
        {
            tree = null;

            var header = (PrefixTreeRootHeader*)parent.DirectRead(treeName);
            if (header == null)
                return tree != null;

            if (header->RootObjectType != RootObjectType.PrefixTree)
                throw new InvalidOperationException("Tried to opened " + treeName + " as a prefix tree, but it is actually a " + header->RootObjectType);

            var state = new PrefixTreeRootMutableState(tx, header);
            tree = new PrefixTree(tx, parent, state, treeName);            
            return tree != null;
        }

        public bool Add(Slice key, long dataPtr)
        {
            // We prepare the signature to compute incrementally. 
            BitVector searchKey = key.ToBitVector();

#if DETAILED_DEBUG
            Console.WriteLine(string.Format("Add(Binary: {1}, Key: {0})", key.ToString(), searchKey.ToBinaryString()));
#endif
            if (Count == 0)
            {
                Leaf* rootLeaf;
                var newNodeName = CreateLeaf(Constants.InvalidNodeName, 0, dataPtr, out rootLeaf);

                // We add the leaf after the head.                  
                Leaf* head = &(_state.Pointer->Head);
                AddAfter(Constants.HeadNodeName, head, 0, rootLeaf);

                _state.RootNodeName = newNodeName;
                _state.Items++; // This will cause the state to set IsModified = true; If this call is removed, add it explicitely                

                return true;
            }

            // TODO: Check if we can use the key instead of the BitVector representation instead. 
            var hashState = Hashing.Iterative.XXHash32.Preprocess(searchKey.Bits);


            // We look for the parent of the exit node for the key.
            var stack = nodesStackPool.Allocate();
            try
            {
                var cutPoint = FindParentExitNode(searchKey, hashState, stack);

                var exitNodeName = cutPoint.Exit;
                var exitNode = this.ModifyNodeByName(exitNodeName);

#if DETAILED_DEBUG        
                Console.WriteLine(string.Format("Parex Node: {0}, Exit Node: {1}, LCP: {2}", cutPoint.Parent != Constants.InvalidNodeName ? this.ToDebugString((Node*)cutPoint.Parent) : "null", this.ToDebugString(exitNode), cutPoint.LongestPrefix));
#endif

                // If the exit node is a leaf and the key is equal to the LCP                 
                if (exitNode->IsLeaf && GetKeySize(((Leaf*)exitNode)->DataPtr) == cutPoint.LongestPrefix)
                    return false; // Then we are done (we found the key already).

                int exitNodeHandleLength = this.GetHandleLength(exitNode);

                bool exitDirection = cutPoint.SearchKey.Get(cutPoint.LongestPrefix);   // Compute the exit direction from the LCP.
                bool isCutLow = cutPoint.LongestPrefix >= exitNodeHandleLength;  // Is this cut point low or high? 
                bool isRightChild = cutPoint.IsRightChild; // Saving this because pointers will get invalidated on update.
                bool isExitNodeRoot = exitNodeName == _state.RootNodeName; // We need to evaluate this before changing the layout.

#if DETAILED_DEBUG
                Console.WriteLine(string.Format("Cut {0}; exit to the {1}", isCutLow ? "low" : "high", exitDirection ? "right" : "left"));
#endif           
                long newInternalName;
                Internal* newInternal;
                long newLeafNodeName;
                Leaf* newLeaf;

                // The new leaf is inserted into the left position.
                newLeafNodeName = CreateLeaf(cutPoint.Parent, (short)(cutPoint.LongestPrefix + 1), dataPtr, out newLeaf);
                // Link the new internal node with the new leaf and the old node.   
                newInternalName = CreateInternal(cutPoint.Parent, exitNode->NameLength, cutPoint.LongestPrefix, out newInternal);
                // Ensure that the right leaf has a 1 in position and the left one has a 0. (TRIE Property)

                if (exitDirection)
                {
                    newInternal->RightPtr = newLeafNodeName;
                    newInternal->JumpRightPtr = newLeafNodeName;

                    newInternal->LeftPtr = cutPoint.Exit;
                    newInternal->JumpLeftPtr = isCutLow && exitNode->IsInternal ? ((Internal*)exitNode)->JumpLeftPtr : cutPoint.Exit;
                }
                else
                {
                    newInternal->LeftPtr = newLeafNodeName;
                    newInternal->JumpLeftPtr = newLeafNodeName;

                    newInternal->RightPtr = cutPoint.Exit;
                    newInternal->JumpRightPtr = isCutLow && exitNode->IsInternal ? ((Internal*)exitNode)->JumpRightPtr : cutPoint.Exit;
                }

                newInternal->ReferencePtr = newLeafNodeName;
                newLeaf->ReferencePtr = newInternalName;

                ValidateInternalNode(newInternalName, newInternal);

                // If the exit node is not the root
                if (isExitNodeRoot)
                {
                    // Then update the root
                    this._state.RootNodeName = newInternalName;
                }
                else
                {
                    var cutPointParentNode = (Internal*)this.ModifyNodeByName(cutPoint.Parent);
                    Debug.Assert(cutPointParentNode->IsInternal);

                    // Update the parent exit node.
                    if (isRightChild)
                    {
                        cutPointParentNode->RightPtr = newInternalName;
                    }
                    else
                    {
                        cutPointParentNode->LeftPtr = newInternalName;
                    }
                }

                // Update the jump table after the insertion.
                if (exitDirection)
                    UpdateRightJumpsAfterInsertion(newInternalName, exitNodeName, isRightChild, newLeafNodeName, newLeaf->NameLength, stack);
                else
                    UpdateLeftJumpsAfterInsertion(newInternalName, exitNodeName, isRightChild, newLeafNodeName, newLeaf->NameLength, stack);

                // If the cut point was low and the exit node internal
                if (isCutLow && exitNode->IsInternal)
                {
#if DETAILED_DEBUG_H
                    Console.WriteLine("Replace Cut-Low");
#endif
                    uint hash = InternalTable.CalculateHashForBits(searchKey, hashState, exitNodeHandleLength);

                    Debug.Assert(exitNodeHandleLength == this.GetHandleLength(exitNode));
                    Debug.Assert(hash == InternalTable.CalculateHashForBits(this.Handle(exitNode), hashState, exitNodeHandleLength));

                    this.NodesTable.Replace(exitNodeName, newInternalName, hash); // Check if this is correct.

                    // TODO: Review the use of short in NameLength and change to ushort. 
                    exitNode->NameLength = (short)(cutPoint.LongestPrefix + 1);

#if DETAILED_DEBUG_H
                    Console.WriteLine("Insert Cut-Low");
#endif

                    hash = InternalTable.CalculateHashForBits(this.Name(exitNode), hashState, this.GetHandleLength(exitNode), cutPoint.LongestPrefix);
                    this.NodesTable.Add(exitNodeName, hash);

                    //  We update the jumps for the exit node.                
                    UpdateJumps(exitNodeName);
                }
                else
                {
                    //  We add the internal node to the jump table.                
                    exitNode->NameLength = (short)(cutPoint.LongestPrefix + 1);

#if DETAILED_DEBUG_H
                    Console.WriteLine("Insert Cut-High");
#endif
                    uint hash = InternalTable.CalculateHashForBits(searchKey, hashState, this.GetHandleLength(newInternal));

                    this.NodesTable.Add(newInternalName, hash);
                }

                // Link the new leaf with it's predecessor and successor.
                if (exitDirection)
                {
                    var rightLeafName = this.GetRightLeaf(exitNodeName);
                    var rightLeaf = (Leaf*)this.ModifyNodeByName(rightLeafName);
                    Debug.Assert(rightLeaf->IsLeaf);

                    AddAfter(rightLeafName, rightLeaf, newLeafNodeName, newLeaf);
                }
                else
                {
                    var leftLeafName = this.GetLeftLeaf(exitNodeName);
                    var leftLeaf = (Leaf*)this.ModifyNodeByName(leftLeafName);
                    Debug.Assert(leftLeaf->IsLeaf);

                    AddBefore(leftLeafName, leftLeaf, newLeafNodeName, newLeaf);
                }

                Debug.Assert(newInternal->JumpLeftPtr != PrefixTree.Constants.InvalidNodeName);
                Debug.Assert(newInternal->JumpRightPtr != PrefixTree.Constants.InvalidNodeName);

                _state.Items++; // This will cause the state to set IsModified = true; If this call is removed, add it explicitely

#if DETAILED_DEBUG
                Console.WriteLine(this.NodesTable.DumpNodesTable(this));
#endif

                return true;
            }
            finally
            {
                stack.Clear();
                nodesStackPool.Free(stack);
            }
        }

        [Conditional("DEBUG")]
        private void ValidateInternalNode(long internalNodeName, Internal* nodePtr)
        {     
            var internalPtr = this.ReadNodeByName(internalNodeName);
            Debug.Assert(nodePtr == internalPtr); // Ensure that trying to read returns the same node. 
            Debug.Assert(nodePtr->IsInternal);

            var extentLength = this.GetExtentLength(nodePtr); // Retrieve the extent of the internal node. 

            // Ensure that the right leaf has a 1 in position and the left one has a 0. (TRIE Property).
            var leftLeaf = ReadNodeByName(nodePtr->LeftPtr);
            var rightLeaf = ReadNodeByName(nodePtr->RightPtr);

            Debug.Assert(this.Name(leftLeaf)[extentLength] == false);
            Debug.Assert(this.Name(rightLeaf)[extentLength] == true);
        }

        private void UpdateRightJumpsAfterInsertion(long insertedNodeName, long exitNodeName, bool isRightChild, long insertedLeafName, long insertedLeafNameLength, Stack<long> stack)
        {
            if (!isRightChild)
            {
                // Not all the jump pointers of 2-fat ancestors need to be updated: actually, we
                // need to update only pointers to nodes that are left descendant of ß.

                while (stack.Count != 0)
                {
                    var toFixNodeName = stack.Pop();
                    var toFix = (Internal*)this.ReadNodeByName(toFixNodeName); // On most cases we just need to read.
                    Debug.Assert(toFix->IsInternal);

                    // TODO: Check this, it doesnt have much sense now that we dont use live pointers.
                    if (toFix->JumpLeftPtr != exitNodeName)
                        break;

                    int jumpLength = this.GetJumpLength( toFix );
                    if (jumpLength < insertedLeafNameLength)
                    {
                        toFix = (Internal*)this.ModifyNodeByName(toFixNodeName); // Now we need to modify
                        toFix->JumpLeftPtr = insertedNodeName;
                    }
                }
            }
            else
            {
                // Not all the jump pointers of 2-fat ancestors need to be updated: actually, we
                // need to update only pointers to nodes that are right descendant of ß.

                while (stack.Count != 0)
                {
                    var toFixNodeName = stack.Peek();
                    var toFix = (Internal*)this.ReadNodeByName(toFixNodeName); // On most cases we just need to read.
                    Debug.Assert(toFix->IsInternal);

                    int jumpLength = this.GetJumpLength(toFix);
                    if (toFix->JumpRightPtr != exitNodeName || jumpLength >= insertedLeafNameLength)
                        break;

                    toFix = (Internal*)this.ModifyNodeByName(toFixNodeName); // Now we need to modify
                    toFix->JumpRightPtr = insertedNodeName;

                    stack.Pop();
                }

                while (stack.Count != 0)
                {
                    var toFixNodeName = stack.Pop();
                    var toFix = (Internal*)this.ReadNodeByName(toFixNodeName); // On most cases we just need to read.
                    Debug.Assert(toFix->IsInternal);

                    var exitNode = this.ReadNodeByName(exitNodeName);
                    while (exitNode->IsInternal && toFix->JumpRightPtr != exitNodeName)
                    {
                        exitNodeName = ((Internal*)exitNode)->JumpRightPtr;
                        exitNode = this.ReadNodeByName(exitNodeName);
                    }

                    // As soon as we cannot find a matching descendant, we can stop updating
                    if (toFix->JumpRightPtr != exitNodeName)
                        return;

                    toFix = (Internal*)this.ModifyNodeByName(toFixNodeName); // Now we need to modify
                    toFix->JumpRightPtr = insertedLeafName;
                }
            }
        }


        private void UpdateLeftJumpsAfterInsertion(long insertedNodeName, long exitNodeName, bool isRightChild, long insertedLeafName, long insertedLeafNameLength, Stack<long> stack)
        {
            // See: Algorithm 2 of [1]

            if (isRightChild)
            {
                // Not all the jump pointers of 2-fat ancestors need to be updated: actually, we
                // need to update only pointers to nodes that are right descendant of ß.

                while (stack.Count != 0)
                {
                    var toFixNodeName = stack.Pop();
                    var toFix = (Internal*)this.ReadNodeByName(toFixNodeName); // On most cases we just need to read.
                    Debug.Assert(toFix->IsInternal);

                    if (toFix->JumpRightPtr != exitNodeName)
                        break;

                    int jumpLength = this.GetJumpLength(toFix);
                    if (jumpLength < insertedLeafNameLength)
                    {
                        toFix = (Internal*)this.ModifyNodeByName(toFixNodeName);
                        toFix->JumpRightPtr = insertedNodeName;
                    }                        
                }
            }
            else
            {
                // Not all the jump pointers of 2-fat ancestors need to be updated: actually, we
                // need to update only pointers to nodes that are left descendant of ß.

                while (stack.Count != 0)
                {
                    var toFixNodeName = stack.Peek();
                    var toFix = (Internal*)this.ReadNodeByName(toFixNodeName); // On most cases we just need to read.
                    Debug.Assert(toFix->IsInternal);

                    int jumpLength = this.GetJumpLength(toFix);

                    if (toFix->JumpLeftPtr != exitNodeName || jumpLength >= insertedLeafNameLength)
                        break;

                    toFix = (Internal*)this.ModifyNodeByName(toFixNodeName);
                    toFix->JumpLeftPtr = insertedNodeName;

                    stack.Pop();
                }

                while (stack.Count != 0)
                {
                    var toFixNodeName = stack.Pop();
                    var toFix = (Internal*)this.ReadNodeByName(toFixNodeName); // On most cases we just need to read.
                    Debug.Assert(toFix->IsInternal);

                    var exitNode = this.ReadNodeByName(exitNodeName);
                    while (exitNode->IsInternal && toFix->JumpLeftPtr != exitNodeName)
                    {
                        exitNodeName = ((Internal*)exitNode)->JumpLeftPtr;
                        exitNode = this.ReadNodeByName(exitNodeName);
                    }

                    // As soon as we cannot find a matching descendant, we can stop updating
                    if (toFix->JumpLeftPtr != exitNodeName)
                        return;

                    toFix = (Internal*)this.ModifyNodeByName(toFixNodeName);
                    toFix->JumpLeftPtr = insertedLeafName;
                }
            }
        }


        private void UpdateJumps(long nodeName)
        {
            var node = (Internal*)this.ModifyNodeByName(nodeName);
            Debug.Assert(node->IsInternal);

            int jumpLength = this.GetJumpLength(node);

            long jumpNodeName = node->LeftPtr;
            Node* jumpNode = this.ReadNodeByName(jumpNodeName);
            while (jumpNode->IsInternal && jumpLength > ((Internal*)jumpNode)->ExtentLength)
            {
                jumpNodeName = ((Internal*)jumpNode)->JumpLeftPtr;
                jumpNode = this.ReadNodeByName(jumpNodeName);
            }

            Debug.Assert(this.Intersects(jumpNode, jumpLength));            
            node->JumpLeftPtr = jumpNodeName;

            jumpNodeName = node->RightPtr;
            jumpNode = this.ReadNodeByName(jumpNodeName);
            while (jumpNode->IsInternal && jumpLength > ((Internal*)jumpNode)->ExtentLength)
            {
                jumpNodeName = ((Internal*)jumpNode)->JumpRightPtr;
                jumpNode = this.ReadNodeByName(jumpNodeName);
            }

            Debug.Assert(this.Intersects(jumpNode, jumpLength));
            node->JumpRightPtr = jumpNodeName;
        }


        private CutPoint FindParentExitNode(BitVector searchKey, Hashing.Iterative.XXHash32Block state, Stack<long> stack)
        {
#if DETAILED_DEBUG
            Console.WriteLine(string.Format("FindParentExitNode({0})", searchKey.ToBinaryString()));
#endif
            // If there is only a single element, then the exit point is the root.
            if (_state.Items == 1)
                return new CutPoint(searchKey.LongestCommonPrefixLength(this.Extent(this.Root)), Constants.InvalidNodeName, _state.RootNodeName, Constants.InvalidNodeName, searchKey);

            int length = searchKey.Count;

            // Find parex(key), exit(key) or fail spectacularly (with very low probability). 
            long parexOrExitNodeName = FatBinarySearch(searchKey, state, stack, -1, length, isExact: false);

            Internal* parexOrExitNode = (Internal*)ReadNodeByName(parexOrExitNodeName);
            Debug.Assert(parexOrExitNode->IsInternal);

            // Check if the node is either the parex(key) and/or exit(key). 
            long candidateNodeName;
            if (parexOrExitNode->ExtentLength < length && searchKey[parexOrExitNode->ExtentLength])
                candidateNodeName = parexOrExitNode->RightPtr;
            else
                candidateNodeName = parexOrExitNode->LeftPtr;

            Node* candidateNode = ReadNodeByName(candidateNodeName);
            int lcpLength = searchKey.LongestCommonPrefixLength(this.Extent(candidateNode));

            // Fat Binary Search just worked with high probability and gave use the parex(key) node. 
            if (this.IsExitNodeOf(candidateNode, searchKey.Count, lcpLength))
                return new CutPoint(lcpLength, parexOrExitNodeName, candidateNodeName, parexOrExitNode->RightPtr, searchKey);

            // We need to find the length of the longest common prefix between the key and the extent of the parex(key).
            lcpLength = Math.Min(parexOrExitNode->ExtentLength, lcpLength);

            Debug.Assert(lcpLength == searchKey.LongestCommonPrefixLength(this.Extent((Node*)parexOrExitNode)));

            long stackTopNodeName;
            Internal* stackTopNode;

            int startPoint;
            if (this.IsExitNodeOf(parexOrExitNode, length, lcpLength))
            {
                // We have the correct exit node, we then must pop it and probably restart the search to find the parent.
                stack.Pop();

                // If the exit node is the root, there is obviously no parent to be found.
                if (parexOrExitNodeName == _state.RootNodeName)
                    return new CutPoint(lcpLength, Constants.InvalidNodeName, parexOrExitNodeName, Constants.InvalidNodeName, searchKey);

                stackTopNodeName = stack.Peek();

                stackTopNode = (Internal*)ReadNodeByName(stackTopNodeName);
                Debug.Assert(stackTopNode->IsInternal);

                startPoint = stackTopNode->ExtentLength;
                if (startPoint == parexOrExitNode->NameLength - 1)
                    return new CutPoint(lcpLength, stackTopNodeName, parexOrExitNodeName, stackTopNode->RightPtr, searchKey);

                // Find parex(key) or fail spectacularly (with very low probability). 
                int stackSize = stack.Count;

                long parexNodeName = FatBinarySearch(searchKey, state, stack, startPoint, parexOrExitNode->NameLength, isExact: false);

                Internal* parexNode = (Internal*)ReadNodeByName(parexNodeName);
                if (parexNode->LeftPtr == parexOrExitNodeName || parexNode->RightPtr == parexOrExitNodeName)
                    return new CutPoint(lcpLength, parexNodeName, parexOrExitNodeName, parexNode->RightPtr, searchKey);

                // It seems we just failed and found an unrelated node, we should restart in exact mode and also clear the stack of what we added during the last search.
                while (stack.Count > stackSize)
                    stack.Pop();

                parexNodeName = FatBinarySearch(searchKey, state, stack, startPoint, parexOrExitNode->NameLength, isExact: true);
                parexNode = (Internal*)ReadNodeByName(parexNodeName);

                return new CutPoint(lcpLength, parexNodeName, parexOrExitNodeName, parexNode->RightPtr, searchKey);
            }

            // The search process failed with very low probability.
            stack.Clear();
            parexOrExitNodeName = FatBinarySearch(searchKey, state, stack, -1, length, isExact: true);
            parexOrExitNode = (Internal*)ReadNodeByName(parexOrExitNodeName);

            if (parexOrExitNode->ExtentLength < length && searchKey[parexOrExitNode->ExtentLength])
                candidateNodeName = parexOrExitNode->RightPtr;
            else
                candidateNodeName = parexOrExitNode->LeftPtr;

            candidateNode = ReadNodeByName(candidateNodeName);

            lcpLength = searchKey.LongestCommonPrefixLength(this.Extent(candidateNode));

            // Fat Binary Search just worked with high probability and gave use the parex(key) node. 
            if (this.IsExitNodeOf(candidateNode, searchKey.Count, lcpLength))
                return new CutPoint(lcpLength, parexOrExitNodeName, candidateNodeName, parexOrExitNode->RightPtr, searchKey);

            stack.Pop();

            // If the exit node is the root, there is obviously no parent to be found.
            if (parexOrExitNodeName == _state.RootNodeName)
                return new CutPoint(lcpLength, Constants.InvalidNodeName, _state.RootNodeName, Constants.InvalidNodeName, searchKey);

            stackTopNodeName = stack.Peek();
            stackTopNode = (Internal*)ReadNodeByName(stackTopNodeName);

            startPoint = stackTopNode->ExtentLength;
            if (startPoint == parexOrExitNode->NameLength - 1)
                return new CutPoint(lcpLength, stackTopNodeName, parexOrExitNodeName, stackTopNode->RightPtr, searchKey);

            long parentNodeName = FatBinarySearch(searchKey, state, stack, startPoint, parexOrExitNode->NameLength, isExact: true);
            var parentNode = (Internal*)ReadNodeByName(parentNodeName);

            return new CutPoint(lcpLength, parentNodeName, parexOrExitNodeName, parentNode->RightPtr, searchKey);
        }

        /// <summary>
        /// Deletion mostly follow the insertion steps uneventfully. [...] To fix the jump pointers, we need to know
        /// the 2-fat ancestors of the parent of parex(x), not of parex(x). Page 171 of [1].
        /// </summary>
        public bool Delete(Slice key, ushort? version = null)
        {
            if (this.Count == 0)
                return false;

            Debug.Assert(this.State.RootNodeName != Constants.InvalidNodeName);

            // We prepare the signature to compute incrementally.
            var searchKey = key.ToBitVector();

            if (this.Count == 1)
            {
                long rootNodeName = _state.RootNodeName;

                // Is the root key (which has to be a Leaf) equal to the one we are looking for?
                var leaf = (Leaf*)this.ReadNodeByName(rootNodeName);
                Debug.Assert(leaf->IsLeaf);
                if (this.Name(leaf).CompareTo(searchKey) != 0)
                    return false;

                RemoveLeaf(rootNodeName);
                _translationTable.DeallocateNodeName(rootNodeName);


                // We remove the root.    
                State.RootNodeName = Constants.InvalidNodeName;
                State.Items--;

                return true;
            }
            
            var hashState = Hashing.Iterative.XXHash32.Preprocess(searchKey.Bits);

            // We look for the parent of the exit node for the key.
            var stack = nodesStackPool.Allocate();
            try
            {                
                var cutPoint = FindParentExitNode(searchKey, hashState, stack);

                long exitNodeName = cutPoint.Exit;
                long parentExitNodeName = cutPoint.Parent;

                // If the exit node is not a leaf or the key is not equal to the LCP             
                Node* exitNode = this.ReadNodeByName(exitNodeName);
                if (exitNode->IsInternal || ReadKey(((Leaf*)exitNode)->DataPtr).ToBitVector().Count != cutPoint.LongestPrefix)
                    return false;

                Debug.Assert(exitNode->IsLeaf); // We are sure that the exit node is a leaf.

                // Then we are done (The key does not exist).
                bool isRightLeaf = cutPoint.IsRightChild;

                var parentExitNode = (Internal*) this.ReadNodeByName(parentExitNodeName);
                Debug.Assert(parentExitNode != null);
                Debug.Assert(parentExitNode->IsInternal);

                long otherNodeName = isRightLeaf ? parentExitNode->LeftPtr : parentExitNode->RightPtr;
                Node* otherNode = this.ModifyNodeByName(otherNodeName);

                // If the parentExitNode is not the root
                // Then we need to fix the grand parent child pointer.
                if (parentExitNodeName != State.RootNodeName)
                {
                    long grandParentExitNodeName = FindGrandParentExitNode(searchKey, hashState, stack);
                    Internal* grandParentExitNode = (Internal*)this.ModifyNodeByName(grandParentExitNodeName);
                    Debug.Assert(grandParentExitNode->IsInternal);

                    isRightLeaf = grandParentExitNode->RightPtr == parentExitNodeName;
                    if (isRightLeaf)
                        grandParentExitNode->RightPtr = otherNodeName;
                    else
                        grandParentExitNode->LeftPtr = otherNodeName;
                }

                int parentExitNodeHandleLength = this.GetHandleLength(parentExitNode);
                int otherNodeHandleLength = this.GetHandleLength(otherNode);

                // If the parent node is the root, then the child becomes the root.
                if (parentExitNodeName == State.RootNodeName)
                    State.RootNodeName = otherNodeName;

                // If the exit node (which should be a leaf) reference is not null 
                long toExitNodePtrName = exitNode->ReferencePtr; 
                if ( toExitNodePtrName != Constants.InvalidNodeName )
                {   
                    var toExitNodePtr = this.ModifyNodeByName(toExitNodePtrName);                                       
                    toExitNodePtr->ReferencePtr = parentExitNode->ReferencePtr; 

                    // There reference just changed now we need to access the new reference node.
                    var forwardReference = this.ModifyNodeByName(toExitNodePtr->ReferencePtr);
                    forwardReference->ReferencePtr = toExitNodePtrName;                    
                }
                else
                {
                    var reference = this.ModifyNodeByName(parentExitNode->ReferencePtr);
                    reference->ReferencePtr = Constants.InvalidNodeName;
                }

                // Delete the leaf and fix it's predecessor and successor references.   
                RemoveLeaf(exitNodeName);                

                // Is this cut point low or high? 
                int t = parentExitNodeHandleLength | otherNodeHandleLength;
                bool isCutLow = (t & -t & otherNodeHandleLength) != 0;  // Is this cut point low or high? 

                // Update the jump table after the deletion.
                if (isRightLeaf)
                    UpdateRightJumpsAfterDeletion(parentExitNodeName, exitNodeName, otherNodeName, isRightLeaf, stack);
                else
                    UpdateLeftJumpsAfterDeletion(parentExitNodeName, exitNodeName, otherNodeName, isRightLeaf, stack);

                // If the cut point was low and the child is internal
                if ( isCutLow && otherNode->IsInternal )
                {
                    //   We remove the existing child node from the jump table
                    uint hash = InternalTable.CalculateHashForBits(this.Name(otherNode), hashState, otherNodeHandleLength, parentExitNode->ExtentLength);
                    this.NodesTable.Remove(otherNodeName, hash);
                    otherNode->NameLength = parentExitNode->NameLength;

                    //   We replace the parent exit node
                    hash = InternalTable.CalculateHashForBits(searchKey, hashState, parentExitNodeHandleLength);
                    this.NodesTable.Replace(parentExitNodeName, otherNodeName, hash);

                    //   We update the jumps table for the child node.
                    UpdateJumps(otherNodeName);
                }
                else
                {
                    //   We remove the parent node from the jump table.
                    otherNode->NameLength = parentExitNode->NameLength;

                    uint hash = InternalTable.CalculateHashForBits(searchKey, hashState, parentExitNodeHandleLength);
                    this.NodesTable.Remove(parentExitNodeName, hash);
                }

                _translationTable.DeallocateNodeName(exitNodeName);
                State.Items--;                

                return true;
            }
            finally
            {
                stack.Clear();
                nodesStackPool.Free(stack);
            }
        }

        private void UpdateLeftJumpsAfterDeletion(long parentExitNodeName, long deletedLeafName, long otherNodeName, bool isRightChild, Stack<long> stack)
        {
            if (isRightChild)
            {
                // Not all the jump pointers of 2-fat ancestors need to be updated: we need to
                // update all nodes jumping right which point to the parent exit node. 
                while (stack.Count != 0)
                {
                    long toFixNodeName = stack.Pop();
                    var toFixNode = (Internal*)this.ReadNodeByName(toFixNodeName);
                    Debug.Assert(toFixNode->IsInternal);

                    if (toFixNode->JumpRightPtr != parentExitNodeName)
                        break;

                    toFixNode = (Internal*)this.ModifyNodeByName(toFixNodeName);
                    toFixNode->JumpRightPtr = otherNodeName;
                }
            }
            else
            {
                while (stack.Count != 0)
                {
                    long toFixNodeName = stack.Peek();
                    var toFixNode = (Internal*)this.ReadNodeByName(toFixNodeName);
                    Debug.Assert(toFixNode->IsInternal);

                    if (toFixNode->JumpLeftPtr != parentExitNodeName)
                        break;

                    toFixNode = (Internal*)this.ModifyNodeByName(toFixNodeName);
                    toFixNode->JumpLeftPtr = otherNodeName;

                    stack.Pop();
                }

                while (stack.Count != 0)
                {
                    long toFixNodeName = stack.Pop();
                    var toFixNode = (Internal*)this.ReadNodeByName(toFixNodeName);
                    Debug.Assert(toFixNode->IsInternal);
                    
                    if (toFixNode->JumpLeftPtr != deletedLeafName)
                            break;

                    Node* otherNode = this.ReadNodeByName(otherNodeName);
                    while (!this.Intersects(otherNode, this.GetJumpLength(toFixNode)))
                    {                        
                        otherNodeName = ((Internal*)otherNode)->JumpLeftPtr;
                        otherNode = this.ReadNodeByName(otherNodeName);
                    }

                    toFixNode = (Internal*)this.ModifyNodeByName(toFixNodeName);
                    toFixNode->JumpLeftPtr = otherNodeName;
                }
            }
        }

        private void UpdateRightJumpsAfterDeletion(long parentExitNodeName, long deletedLeafName, long otherNodeName, bool isRightChild, Stack<long> stack)
        {
            if (!isRightChild)
            {
                // Not all the jump pointers of 2-fat ancestors need to be updated: we need to
                // update all nodes jumping left which point to the parent exit node. 
                while (stack.Count != 0)
                {
                    long toFixNodeName = stack.Pop();
                    var toFixNode = (Internal*)this.ReadNodeByName(toFixNodeName);
                    Debug.Assert(toFixNode->IsInternal);

                    if (toFixNode->JumpLeftPtr != parentExitNodeName)
                        break;

                    toFixNode = (Internal*)this.ModifyNodeByName(toFixNodeName);
                    toFixNode->JumpLeftPtr = otherNodeName;
                }
            }
            else
            {
                while (stack.Count != 0)
                {
                    long toFixNodeName = stack.Peek();
                    var toFixNode = (Internal*)this.ReadNodeByName(toFixNodeName);

                    if (toFixNode->JumpRightPtr != parentExitNodeName)
                        break;

                    toFixNode = (Internal*)this.ModifyNodeByName(toFixNodeName);
                    toFixNode->JumpRightPtr = otherNodeName;
                    stack.Pop();
                }

                while (stack.Count != 0)
                {
                    long toFixNodeName = stack.Pop();
                    var toFixNode = (Internal*)this.ReadNodeByName(toFixNodeName);

                    if (toFixNode->JumpRightPtr != deletedLeafName)
                        break;

                    Node* otherNode = this.ReadNodeByName(otherNodeName);
                    while (!this.Intersects(otherNode, this.GetJumpLength(toFixNode)))
                    {
                        otherNodeName = ((Internal*)otherNode)->JumpRightPtr;
                        otherNode = this.ReadNodeByName(otherNodeName);
                    }

                    toFixNode = (Internal*)this.ModifyNodeByName(toFixNodeName);
                    toFixNode->JumpRightPtr = otherNodeName;
                }
            }
        }

        private long FindGrandParentExitNode(BitVector searchKey, Hashing.Iterative.XXHash32Block hashState, Stack<long> stack)
        {
            Debug.Assert(State.Items != 0);

            long parentExitNodeName = stack.Pop();

            // The parent is the root, therefore there is no grandparent. 
            if (parentExitNodeName == State.RootNodeName)
                return Constants.InvalidNodeName;

            var parentExitNode = this.ReadNodeByName(parentExitNodeName);

            long topName = stack.Peek();
            var top = (Internal*)this.ReadNodeByName(topName);

            int start = top->ExtentLength;
            if (start == parentExitNode->NameLength - 1)
                return topName;

            int stackSize = stack.Count;

            // We will find the proper grand parent exit node with very high probability.
            long grandParentExitNodeName = FatBinarySearch(searchKey, hashState, stack, start, parentExitNode->NameLength, false);

            var grandParentExitNode = (Internal*)this.ReadNodeByName(grandParentExitNodeName);
            if (grandParentExitNode->RightPtr == parentExitNodeName || grandParentExitNode->LeftPtr == parentExitNodeName)
                return grandParentExitNodeName;

            // We had failed spectacularly. Ensure there is no garbage on the stack and clean it up.
            while (stack.Count > stackSize)
                stack.Pop();

            Debug.Assert(stack.Count == stackSize);

            return FatBinarySearch(searchKey, hashState, stack, start, parentExitNode->NameLength, true);
        }

        public bool TryGet(Slice key, out long value)
        {
            if (Count == 0)
            {
                value = -1;
                return false;
            }

            // We look for the parent of the exit node for the key.
            var exitNode = FindExitNode(key);
            var node = ReadNodeByName(exitNode.Exit);

            // If the exit node is a leaf and the key is equal to the LCP 
            if (node->IsLeaf && GetKeySize(((Leaf*)node)->DataPtr) == exitNode.LongestPrefix)
            {
                value = ((Leaf*)node)->DataPtr;

                return true; // Then we are done (we found the key already).
            }

            value = -1;
            return false;
        }

        public bool Contains(Slice key)
        {
            if (Count == 0)
                return false;

            // We look for the parent of the exit node for the key.
            var exitNode = FindExitNode(key);
            var node = ReadNodeByName(exitNode.Exit);

            // If the exit node is a leaf and the key is equal to the LCP 
            if (node->IsLeaf && GetKeySize(((Leaf*)node)->DataPtr) == exitNode.LongestPrefix)
                return true; // Then we are done (we found the key already).

            return false;
        }

        public Slice Successor(Slice key)
        {
            if (Count == 0)
                return Slice.AfterAllKeys;

            var nodeName = SuccessorInternal(key);
            if (nodeName == Constants.TailNodeName)
                return Slice.AfterAllKeys;

            var node = (Leaf*)ReadNodeByName(nodeName);
            Debug.Assert(node->IsLeaf); // Linked list elements are always leaves.
            return this.ReadKey(node->DataPtr);
        }

        public Slice Predecessor(Slice key)
        {
            if (Count == 0)
                return Slice.BeforeAllKeys;

            var nodeName = PredecessorInternal(key);
            if (nodeName == Constants.HeadNodeName)
                return Slice.BeforeAllKeys;

            var node = (Leaf*)ReadNodeByName(nodeName);
            Debug.Assert(node->IsLeaf); // Linked list elements are always leaves.
            return this.ReadKey(node->DataPtr);
        }

        public Slice FirstKey()
        {
            if (Count == 0)
                return Slice.BeforeAllKeys;

            Debug.Assert(_state.Head.PreviousPtr == Constants.InvalidNodeName);
            Debug.Assert(_state.Head.NextPtr != Constants.InvalidNodeName);

            var refHead = (Leaf*)ReadNodeByName(_state.Head.NextPtr);
            Debug.Assert(refHead->IsLeaf); // Linked list elements are always leaves.
            return this.ReadKey(refHead->DataPtr);
        }

        public Slice LastKey()
        {
            if (Count == 0)
                return Slice.AfterAllKeys;

            Debug.Assert(_state.Tail.PreviousPtr != Constants.InvalidNodeName);
            Debug.Assert(_state.Tail.NextPtr == Constants.InvalidNodeName);

            var refTail = (Leaf*)ReadNodeByName(_state.Tail.PreviousPtr);
            Debug.Assert(refTail->IsLeaf); // Linked list elements are always leaves.
            return this.ReadKey(refTail->DataPtr);
        }

        public long Count => _state.Items;

        internal Node* Root => this.ReadNodeByName(_state.RootNodeName);
        internal PrefixTreeRootMutableState State => _state;
        internal PrefixTreeTranslationTableMutableState TranslationTable => _translationTable;
        internal InternalTable NodesTable => this._table;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long SuccessorInternal(Slice key)
        {
            // x+ = min{y ? S | y = x} (the successor of x in S) - Page 160 of [1]

            // We look for the exit node for the key
            var exitFound = FindExitNode(key);

            var exitNode = this.ReadNodeByName(exitFound.Exit);
            var exitNodeName = exitFound.Exit;

            // We compare the key with the exit node extent.
            int dummy;
            if (exitFound.SearchKey.CompareToInline(this.Extent(exitNode), out dummy) <= 0)
            {
                // If the key is smaller than the extent, we exit to the left leaf.
                return this.GetLeftLeaf(exitNodeName);
            }
            else
            {
                // If the key is greater than the extent, we exit to the right leaf and get the next.
                var nodeRefName = this.GetRightLeaf(exitNodeName);
                var nodeRef = (Leaf*)this.ReadNodeByName(nodeRefName);
                var leafRefName = nodeRef->NextPtr;
                Debug.Assert(!this.ReadNodeByName(leafRefName)->IsInternal);

                return leafRefName;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long PredecessorInternal(Slice key)
        {
            // x- = max{y ? S | y < x} (the predecessor of x in S) - Page 160 of [1]

            // We look for the exit node for the key
            var exitFound = FindExitNode(key);

            var exitNode = this.ReadNodeByName(exitFound.Exit);
            var exitNodeName = exitFound.Exit;

            // We compare the key with the exit node extent.
            int dummy;
            if (this.Extent(exitNode).CompareToInline(exitFound.SearchKey, out dummy) < 0)
            {
                // If the key is greater than the extent, we exit to the right leaf.             
                return this.GetRightLeaf(exitNodeName);
            }
            else
            {
                // If the key is smaller than the extent, we exit to the left leaf and get the previous leaf.
                var nodeRefName = this.GetLeftLeaf(exitNodeName);
                var nodeRef = (Leaf*)this.ReadNodeByName(nodeRefName);
                var leafRefName = nodeRef->PreviousPtr;
                Debug.Assert(!this.ReadNodeByName(leafRefName)->IsInternal);

                return leafRefName;
            }
        }

        private ExitNode FindExitNode(Slice key)
        {
            Debug.Assert(Count != 0);

            // We prepare the signature to compute incrementally. 
            BitVector searchKey = key.ToBitVector();

            if (Count == 1)
                return new ExitNode(searchKey.LongestCommonPrefixLength(this.Extent(this.Root)), _state.RootNodeName, searchKey);

            // We look for the parent of the exit node for the key.
            var state = Hashing.Iterative.XXHash32.Preprocess(searchKey.Bits);

            // Find parex(key), exit(key) or fail spectacularly (with very low probability). 
            long parexOrExitNodeName = FatBinarySearch(searchKey, state, -1, searchKey.Count, isExact: false);
            Internal* parexOrExitNode = (Internal*)ReadNodeByName(parexOrExitNodeName);
            Debug.Assert(parexOrExitNode->IsInternal);

            // Check if the node is either the parex(key) and/or exit(key). 
            long candidateNodeName;
            if (parexOrExitNode->ExtentLength < searchKey.Count && searchKey[parexOrExitNode->ExtentLength])
                candidateNodeName = parexOrExitNode->RightPtr;             
            else
                candidateNodeName = parexOrExitNode->LeftPtr;

            Node* candidateNode = ReadNodeByName(candidateNodeName);

            int lcpLength = searchKey.LongestCommonPrefixLength(this.Extent(candidateNode));

            // Fat Binary Search just worked with high probability and gave use the parex(key) node. 
            if (this.IsExitNodeOf(candidateNode, searchKey.Count, lcpLength))
                return new ExitNode(lcpLength, candidateNodeName, searchKey);

            lcpLength = Math.Min(parexOrExitNode->ExtentLength, lcpLength);
            if (this.IsExitNodeOf(parexOrExitNode, searchKey.Count, lcpLength))
                return new ExitNode(lcpLength, parexOrExitNodeName, searchKey);

            // With very low priority we screw up and therefore we start again but without skipping anything. 
            parexOrExitNodeName = FatBinarySearch(searchKey, state, -1, searchKey.Count, isExact: true);
            parexOrExitNode = (Internal*)ReadNodeByName(parexOrExitNodeName);
            Debug.Assert(parexOrExitNode->IsInternal);

            if (this.Extent((Node*)parexOrExitNode).IsProperPrefix(searchKey))
            {
                if (parexOrExitNode->ExtentLength < searchKey.Count && searchKey[parexOrExitNode->ExtentLength])
                    candidateNodeName = parexOrExitNode->RightPtr;
                else
                    candidateNodeName = parexOrExitNode->LeftPtr;
            }
            else
            {
                candidateNodeName = parexOrExitNodeName;
            }

            return new ExitNode(searchKey.LongestCommonPrefixLength(this.Extent(candidateNode)), candidateNodeName, searchKey);
        }

        private unsafe long FatBinarySearch(BitVector searchKey, Hashing.Iterative.XXHash32Block state, Stack<long> stack, int startBit, int endBit, bool isExact)
        {
            Debug.Assert(searchKey != null);
            Debug.Assert(state != null);
            Debug.Assert(startBit < endBit - 1);
            Debug.Assert(stack != null);

#if DETAILED_DEBUG
            Console.WriteLine(string.Format("FatBinarySearch({0},{1},({2}..{3})", searchKey.ToDebugString(), DumpStack(stack), startBit, endBit));
#endif
            endBit--;

            long top = Constants.InvalidNodeName;
            if (stack.Count != 0)
                top = stack.Peek();

            if (startBit == -1)
            {
                top = _state.RootNodeName;
                stack.Push(top);

                var topNode = (Internal*)ReadNodeByName(top);
                Debug.Assert(topNode->IsInternal);
                startBit = topNode->ExtentLength;
            }

            var nodesTable = this.NodesTable;

            uint checkMask = (uint)(-1 << Bits.CeilLog2(endBit - startBit));
            while (endBit - startBit > 0)
            {
                Debug.Assert(checkMask != 0);

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
                    uint hash = InternalTable.CalculateHashForBits(searchKey, state, current);

                    int position = isExact ? nodesTable.GetExactPosition(searchKey, current, hash)
                                           : nodesTable.GetPosition(searchKey, current, hash);

                    long itemPtr = position != -1 ? nodesTable[position] : Constants.InvalidNodeName;
                    if (itemPtr == Constants.InvalidNodeName)
                    {
#if DETAILED_DEBUG
                        Console.WriteLine("Missing " + ((isExact) ? "exact" : "non exact"));
#endif
                        endBit = current - 1;
                    }
                    else
                    {

                        Internal* item = (Internal*)ReadNodeByName(itemPtr);
                        Debug.Assert(item->IsInternal); // Make sure there are only internal nodes there. 

                        if (item->ExtentLength < current)
                        {
#if DETAILED_DEBUG
                            Console.WriteLine("Missing " + ((isExact) ? "exact" : "non exact"));
#endif
                            endBit = current - 1;
                        }
                        else
                        {
#if DETAILED_DEBUG
                            Console.WriteLine("Found " + ((isExact) ? "exact" : "non exact") + " extent of length " + item->ExtentLength + " with GetExtentLength of " + this.GetExtentLength(item));
#endif
                            // Add it to the stack, update search and continue
                            top = itemPtr;
                            stack.Push(top);

                            startBit = item->ExtentLength;
                        }
                    }
                }

                checkMask >>= 1;
            }

#if DETAILED_DEBUG
            Console.WriteLine(string.Format("Final interval: ({0}..{1}); Top: {2}; Stack: {3}", startBit, endBit + 1, this.ToDebugString((Node*)top), DumpStack(stack)));
#endif
            return top;
        }

        private string DumpStack(Stack<long> stack)
        {
            var builder = new StringBuilder();
            builder.Append("[");

            bool first = true;
            foreach (var nodeName in stack)
            {
                Node* node = this.ReadNodeByName(nodeName);
                if (!first)
                    builder.Append(", ");

                builder.Append(this.ToDebugString(node));

                first = false;
            }

            builder.Append("] ");

            return builder.ToString();
        }

        private unsafe long FatBinarySearch(BitVector searchKey, Hashing.Iterative.XXHash32Block state, int startBit, int endBit, bool isExact)
        {
            Debug.Assert(searchKey != null);
            Debug.Assert(state != null);
            Debug.Assert(startBit < endBit - 1);

#if DETAILED_DEBUG
            Console.WriteLine(string.Format("FatBinarySearch({0},({1}..{2})", searchKey.ToDebugString(), startBit, endBit));
#endif
            endBit--;

            long top = Constants.InvalidNodeName;

            if (startBit == -1)
            {
                Debug.Assert(this.Root->IsInternal);

                top = _state.RootNodeName;
                startBit = ((Internal*)ReadNodeByName(top))->ExtentLength;
            }

            var nodesTable = this.NodesTable;

            uint checkMask = (uint)(-1 << Bits.CeilLog2(endBit - startBit));
            while (endBit - startBit > 0)
            {
                Debug.Assert(checkMask != 0);

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
                    uint hash = InternalTable.CalculateHashForBits(searchKey, state, current);

                    int position = isExact ? nodesTable.GetExactPosition(searchKey, current, hash)
                                           : nodesTable.GetPosition(searchKey, current, hash);


                    long itemPtr = position != -1 ? nodesTable[position] : Constants.InvalidNodeName;
                    if (itemPtr == Constants.InvalidNodeName)
                    {
#if DETAILED_DEBUG
                        Console.WriteLine("Missing " + ((isExact) ? "exact" : "non exact"));
#endif
                        endBit = current - 1;
                    }
                    else
                    {
                        Internal* item = (Internal*)ReadNodeByName(itemPtr);
                        Debug.Assert(item->IsInternal); // Make sure there are only internal nodes there. 

                        if (item->ExtentLength < current)
                        {
#if DETAILED_DEBUG
                            Console.WriteLine("Missing " + ((isExact) ? "exact" : "non exact"));
#endif
                            endBit = current - 1;
                        }
                        else
                        {
#if DETAILED_DEBUG
                            Console.WriteLine("Found " + ((isExact) ? "exact" : "non exact") + " extent of length " + item->ExtentLength + " with GetExtentLength of " + this.GetExtentLength(item));
#endif
                            // Add it to the stack, update search and continue
                            top = itemPtr;

                            startBit = item->ExtentLength;
                        }
                    }
                }

                checkMask >>= 1;
            }

#if DETAILED_DEBUG
            Console.WriteLine(string.Format("Final interval: ({0}..{1}); Top: {2}", startBit, endBit + 1, this.ToDebugString(this.ReadNodeByName(top))));
#endif
            return top;
        }

        internal Node* ReadNodeByName(long nodeName)
        {
            if (nodeName == Constants.InvalidNodeName)
                return null;

            if (PrefixTree.IsTombstone(nodeName))
            {
                if (nodeName == Constants.HeadNodeName)
                {
                    return (Node*)&(_state.Pointer->Head);
                }
                else
                {
                    Debug.Assert(nodeName == Constants.TailNodeName);
                    return (Node*)&(_state.Pointer->Tail);
                }
            }

            Debug.Assert(nodeName > Constants.InvalidNodeName);

            var location = _translationTable.MapVirtualToPhysical(nodeName);
            if (location.PageNumber == Constants.InvalidPage)
                return null;

            // TODO: Cache last access, it may be the very same page.

            var page = _tx.GetPage(location.PageNumber).ToPrefixTreePage();
            return page.GetNodePointer(location.NodeOffset);
        }

        private Node* ModifyNodeByName(long nodeName)
        {            
            if (IsTombstone(nodeName))
            {
                // We will be modifying the data after this call. If it is a tombstone, then we should handle it appropriately anyways.
                _state.IsModified = true;

                if (nodeName == Constants.HeadNodeName)
                {
                    return (Node*)&(_state.Pointer->Head);
                }
                else
                {
                    Debug.Assert(nodeName == Constants.TailNodeName);
                    return (Node*)&(_state.Pointer->Tail);
                }
            }

            Debug.Assert(nodeName > Constants.InvalidNodeName);

            var location = _translationTable.MapVirtualToPhysical(nodeName);
            if (location.PageNumber == Constants.InvalidPage)
                return null;

            // TODO: Cache last access, it may be the very same page.

            var page = _tx.ModifyPage(location.PageNumber).ToPrefixTreePage();
            return page.GetNodePointer(location.NodeOffset);
        }

        private static bool IsTombstone(long nodeName)
        {
            return nodeName < PrefixTree.Constants.TombstoneNodeName;                
        }

        private long CreateInternal(long parentNode, short nameLength, short extentLength, out Internal* ptr)
        {
            long nodeName = _translationTable.AllocateNodeName(parentNode);

            var location = _translationTable.MapVirtualToPhysical(nodeName);
            PrefixTreePage page = _tx.ModifyPage(location.PageNumber).ToPrefixTreePage();

            ptr = (Internal*)page.GetNodePointer(location.NodeOffset);
            Debug.Assert(ptr->Type == NodeType.Uninitialized);
            ptr->Initialize(nameLength, extentLength);

            return nodeName;
        }

        private long CreateLeaf(long parentNode, short nameLength, long dataPtr, out Leaf* ptr)
        {
            long nodeName = _translationTable.AllocateNodeName(parentNode);

            var location = _translationTable.MapVirtualToPhysical(nodeName);
            PrefixTreePage page = _tx.ModifyPage(location.PageNumber).ToPrefixTreePage();            

            ptr = (Leaf*)page.GetNodePointer(location.NodeOffset);
            Debug.Assert(ptr->Type == 0);
            ptr->Initialize(nameLength);
            ptr->DataPtr = dataPtr;

            Debug.Assert(page.FreeSpace.Get((int)location.NodeOffset) == false);

            return nodeName;
        }

        private void AddBefore(long successorName, Leaf* successor, long newNodeName, Leaf* newNode)
        {
            newNode->PreviousPtr = successor->PreviousPtr;
            newNode->NextPtr = successorName;

            var previousNode = (Leaf*)this.ModifyNodeByName(successor->PreviousPtr);
            Debug.Assert(previousNode->IsLeaf || previousNode->IsTombstone);

            previousNode->NextPtr = newNodeName;
            successor->PreviousPtr = newNodeName;
        }

        private void AddAfter(long predecessorName, Leaf* predecessor, long newNodeName, Leaf* newNode)
        {
            newNode->NextPtr = predecessor->NextPtr;
            newNode->PreviousPtr = predecessorName;

            var nextNode = (Leaf*)this.ModifyNodeByName(predecessor->NextPtr);
            Debug.Assert(nextNode->IsLeaf || nextNode->IsTombstone);

            nextNode->PreviousPtr = newNodeName;
            predecessor->NextPtr = newNodeName;
        }

        private void RemoveLeaf(long nodeName)
        {
            var node = (Leaf*)this.ModifyNodeByName(nodeName);
            Debug.Assert(node->IsLeaf);

            var previousNode = (Leaf*)this.ModifyNodeByName(node->PreviousPtr);
            var nextNode = (Leaf*)this.ModifyNodeByName(node->NextPtr);
            Debug.Assert(previousNode->IsLeaf || previousNode->IsTombstone);
            Debug.Assert(nextNode->IsLeaf || nextNode->IsTombstone);

            nextNode->PreviousPtr = node->PreviousPtr;
            previousNode->NextPtr = node->NextPtr;            
        }


        internal Slice ReadKey(long dataPtr)
        {
            int size;
            var pointer = RawDataSection.DirectRead(_tx, dataPtr, out size);
            var reader = new TableValueReader(pointer, size);

            int keySize;
            var keyPtr = reader.Read(0, out keySize);

            return new Slice(keyPtr, (ushort)keySize);                  
        }

        internal int GetKeySize(long dataPtr)
        {
            int size;
            var pointer = RawDataSection.DirectRead(_tx, dataPtr, out size);
            var reader = new TableValueReader(pointer, size);

            int keySize;
            var keyPtr = reader.Read(0, out keySize);

            // The key is written without the prefix. But for the bitvector calculations that means we need the 
            // prefix version size. 
            return ( keySize + 2 ) * BitVector.BitsPerByte;
        }
    }
}

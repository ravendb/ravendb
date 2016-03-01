using Sparrow;
using Sparrow.Binary;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Voron.Impl;

namespace Voron.Data.Compact
{
    //[StructLayout(LayoutKind.Explicit, Size = 16, Pack = 1)]
    [StructLayout(LayoutKind.Sequential)]
    public struct PrefixTreeTranslationTableHeader
    {
        public long PageNumber;        

        public long Items;

        public long ChunkSize;

        public long NodesPerChunk;

        public long InnerNodesPerChunk;        
    }

    [StructLayout(LayoutKind.Explicit, Size = 16, Pack = 1)]
    public struct PrefixTreeNodeLocationPtr
    {
        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public long NodeOffset;

        public bool IsValid
        {
            get { return PageNumber != -1; }
        }
    }

    public unsafe class PrefixTreeTranslationTableMutableState
    {        
        private readonly LowLevelTransaction _tx;
        private readonly PrefixTreeRootMutableState _header;
        private PrefixTreeTranslationTableHeader _innerCopy;
        private readonly PageLocator _pageLocator;

        private bool _isModified;

        public PrefixTreeTranslationTableMutableState(LowLevelTransaction tx, PrefixTreeRootMutableState state)
        {
            Debug.Assert(tx != null);

            this._tx = tx;
            this._header = state;
            this._innerCopy = state.Pointer->TranslationTable;
            this._pageLocator = new PageLocator(tx);
        }

        /// <summary>
        /// The translation table page number where the data is really stored. 
        /// </summary>
        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _innerCopy.PageNumber; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _innerCopy.PageNumber = value;
                IsModified = true;
            }
        }

        /// <summary>
        /// How mnay nodes are stored per chunk. 
        /// </summary>
        public long NodesPerChunk
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _innerCopy.NodesPerChunk; }
        }

        /// <summary>
        /// How many inner nodes are stored per chunk. 
        /// </summary>
        public long InnerNodesPerChunk
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _innerCopy.InnerNodesPerChunk; }
        }

        /// <summary>
        /// This is the amount of pages/subtrees already reserved in the translation table for the tree.
        /// </summary>
        public long Items
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _innerCopy.Items; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _innerCopy.Items = value;
                IsModified = true;
            }
        }

        public bool IsModified
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _isModified; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                if (_tx.Flags != TransactionFlags.ReadWrite)
                    throw new InvalidOperationException("Invalid operation outside of a write transaction");
                _isModified = value;
            }
        }

        public void CopyTo( PrefixTreeRootHeader* header)
        {
            header->TranslationTable = _innerCopy;
        }

        public PtrBitVector GetFreeSpaceTable( Page page )
        {
            byte* location = page.DataPointer + _innerCopy.Items * sizeof(long);
            return new PtrBitVector( location, (int) _innerCopy.Items);
        }

        public void Initialize ( int chunkTreeDepth )
        {
            _innerCopy.NodesPerChunk = (int)Math.Pow(2, chunkTreeDepth) - 1;
            _innerCopy.InnerNodesPerChunk = (int)Math.Pow(2, chunkTreeDepth - 1) - 1;
            _innerCopy.ChunkSize = _innerCopy.NodesPerChunk * sizeof(PrefixTree.Node) + (_innerCopy.NodesPerChunk / BitVector.BitsPerByte) + 1;
            _innerCopy.Items = PrefixTree.Constants.TranslationTableInitialItems;

            if (_innerCopy.NodesPerChunk >= ushort.MaxValue - 1)
                throw new NotSupportedException($"Requesting more than {ushort.MaxValue - 2} nodes per chunk is not supported.");

            int freeSpaceTableSize = (int) (_innerCopy.Items / BitVector.BitsPerByte) + 1;

            // We will allocate at least two pages to force an overflow page.
            var _table = _tx.AllocateOverflowPage<PageHeader>((int) (_innerCopy.Items * sizeof(long)) + freeSpaceTableSize);
            Debug.Assert(_table.IsOverflow); // Making sure we got an overflow page.
            _innerCopy.PageNumber = _table.PageNumber;

            // We are actually filling the page mapping to PrefixTree.Constants.InvalidPage and also the free space as all free in a single call :)
            Memory.SetInline(_table.DataPointer, 0xFF, _table.OverflowSize);

            Debug.Assert(((long*)_table.DataPointer)[0] == PrefixTree.Constants.InvalidPage); // Make sure we are writing invalid page in memory.
            Debug.Assert(((long*)_table.DataPointer)[_table.OverflowSize / sizeof(long) - 1] == PrefixTree.Constants.InvalidPage); // Make sure we are writing invalid page in memory.            

            IsModified = true;
        }

        public bool AreNodesInSameChunk( long na, long nb )
        {
            long naChunkIdx = na / _innerCopy.NodesPerChunk;
            long nbChunkIdx = nb / _innerCopy.NodesPerChunk;

            return naChunkIdx == nbChunkIdx;
        }

        private PrefixTreePage AllocateChunk(long chunkIdx)
        {
            Debug.Assert(chunkIdx != PrefixTree.Constants.InvalidPage);

            // We need to allocate a new chunk page and store the physical page number in here.
            var page = _tx.AllocateOverflowPage<PrefixTreePageHeader>(_innerCopy.ChunkSize);

            var chunk = page.ToPrefixTreePage();
            chunk.Chunk = chunkIdx;
            chunk.NodesPerChunk = (ushort)_innerCopy.NodesPerChunk;
            chunk.Initialize();

            Debug.Assert(page.IsOverflow); // Making sure we got an overflow page (and didnt messed up the format).

            return chunk;
        }

        private long AllocateNodeInChunkSlow()
        {
            if (!_tablePtr.IsValid || _innerCopy.PageNumber != _tablePtr.PageNumber)
                _tablePtr = new PageHandlePtr(_pageLocator.GetReadOnlyPage(_innerCopy.PageNumber), false);

            var table = _tablePtr.Value;
            int chunkId = GetFreeSpaceTable(table).FindLeadingOne();
            while (chunkId >= 0)
            {               
                long name;
                if ( TryAllocateNodeInChunk(chunkId * _innerCopy.NodesPerChunk, out name))
                {
                    Debug.Assert(((long*)_pageLocator.GetReadOnlyPage(_innerCopy.PageNumber).DataPointer)[chunkId] != -1);
                    return name;
                }    
                else
                {
                    if (!_tablePtr.IsWritable || _innerCopy.PageNumber != _tablePtr.PageNumber)
                        _tablePtr = new PageHandlePtr(_pageLocator.GetWritablePage(_innerCopy.PageNumber), true);

                    table = _tablePtr.Value;
                    GetFreeSpaceTable(table).Set(chunkId, false);
                }

                chunkId = GetFreeSpaceTable(table).FindLeadingOne();
            }

            // We need to allocate a larger table and update the relevant pages.
            // Allocate page with one more overflow.
            // Copy the current table into the new page
            // Initialize with Constant.InvalidPage the rest of the table.             
            // Update the header with the new page for the table. 
            // Free the old page. 
            // Set the modified flag on.    
            this.IsModified = true;

            throw new NotImplementedException();
        }

        private PageHandlePtr _tablePtr; 

        private bool TryAllocateNodeInChunk( long parentName, out long name )
        {
            int chunkIdx = (int)(parentName / _innerCopy.NodesPerChunk);

            if (!_tablePtr.IsValid || _innerCopy.PageNumber != _tablePtr.PageNumber)
                _tablePtr = new PageHandlePtr(_pageLocator.GetReadOnlyPage(_innerCopy.PageNumber), false);

            var table = _tablePtr.Value; // We are in read mode, no intention to modify yet (therefore no copy wasted)

            var freeSpace = GetFreeSpaceTable(table);
            if (freeSpace.Get(chunkIdx)) // We still have free space reported here.
            {
                PrefixTreePage chunkPage;

                // We will retrieve the chunks mapping table.
                long chunkPageNumber = ((long*)table.DataPointer)[chunkIdx];
                if ( chunkPageNumber == PrefixTree.Constants.InvalidPage )
                {
                    // Perform the actual chunk allocation because it is still not allocated. 
                    chunkPage = AllocateChunk(chunkIdx);
                    chunkPageNumber = chunkPage.PageNumber;

                    // Update the physical page, therefore we need to get a copy that will be eventually committed. 
                    if (!_tablePtr.IsWritable || _innerCopy.PageNumber != _tablePtr.PageNumber)
                        _tablePtr = new PageHandlePtr(_pageLocator.GetWritablePage(_innerCopy.PageNumber), true);

                    table = _tablePtr.Value;
                    ((long*)table.DataPointer)[chunkIdx] = chunkPageNumber;

                    Debug.Assert(GetFreeSpaceTable(table).Get(chunkIdx) == true);
                }
                else
                {
                    chunkPage = _pageLocator.GetWritablePage(chunkPageNumber).ToPrefixTreePage();
                }

                // We will try to allocate from the chunk free space.
                int idx = chunkPage.FreeSpace.FindLeadingOne();
                if (idx < 0) // Check if we have space available. 
                {
                    // We dont have, so we get the top level free space and set it as complete.
                    if (!_tablePtr.IsWritable || _innerCopy.PageNumber != _tablePtr.PageNumber)
                        _tablePtr = new PageHandlePtr(_pageLocator.GetWritablePage(_innerCopy.PageNumber), true);

                    table = _tablePtr.Value;
                    GetFreeSpaceTable(table).Set(chunkIdx, false);

                    name = PrefixTree.Constants.InvalidNodeName;
                    return false;
                }
                
                // We can allocate, so we open the page for writing (we will pay the modify now and cache it at the transaction level). 
                chunkPage = _pageLocator.GetWritablePage(chunkPageNumber).ToPrefixTreePage();
                chunkPage.FreeSpace.Set(idx, false); // We mark the node as used.

                name = chunkIdx * _innerCopy.NodesPerChunk + idx; // Convert relative naming to virtual node name.
                return true;
            }

            name = PrefixTree.Constants.InvalidNodeName;
            return false;
        }

        public long AllocateNodeName( long parentName = PrefixTree.Constants.InvalidNodeName)
        {
            if ( _tx.Flags == TransactionFlags.Read )
                throw new InvalidOperationException("Cannot allocate a node in a read transaction.");

            long nodeName = PrefixTree.Constants.InvalidNodeName;
            if (parentName != PrefixTree.Constants.InvalidNodeName && TryAllocateNodeInChunk(parentName, out nodeName))
                return nodeName;

            return AllocateNodeInChunkSlow();
        }

        public void DeallocateNodeName(long nodeName)
        {
            int chunkIdx = (int)(nodeName / _innerCopy.NodesPerChunk);
            int nodeIdx = (int)(nodeName % _innerCopy.NodesPerChunk);

            var _table = _pageLocator.GetReadOnlyPage(_innerCopy.PageNumber); // We are in read mode, no intention to modify yet (therefore no copy wasted)            

            // We will retrieve the chunks mapping table.
            long chunkPageNumber = ((long*)_table.DataPointer)[chunkIdx];
            Debug.Assert(chunkPageNumber != PrefixTree.Constants.InvalidPage);

            // We can allocate, so we open the page for writing (we will pay the modify now and cache it at the transaction level). 
            var chunkPage = _pageLocator.GetWritablePage(chunkPageNumber).ToPrefixTreePage();

            // We mark the node as unused.
            chunkPage.FreeSpace.Set(nodeIdx, true); 

            if (!GetFreeSpaceTable(_table).Get(chunkIdx)) // It was full.
            {
                // Now we are going to be modifying the table.
                _table = _pageLocator.GetWritablePage(_innerCopy.PageNumber);
                // We mark the chunk as having free space 
                GetFreeSpaceTable(_table).Set(chunkIdx, true); 
            }            
        }

        public PrefixTreeNodeLocationPtr MapVirtualToPhysical(long nodeName)
        {
            Debug.Assert(nodeName > PrefixTree.Constants.InvalidNodeName);

            if (nodeName <= PrefixTree.Constants.InvalidNodeName) // This shouldnt happen at all. 
                throw new InvalidOperationException("Cannot map tombstones and invalid node names into physical locations.");

            if (_innerCopy.PageNumber == PrefixTree.Constants.InvalidPage) // This shouldnt happen at all. 
                throw new InvalidOperationException("Mapping table has not being initialized.");

            if (!_tablePtr.IsValid || _innerCopy.PageNumber != _tablePtr.PageNumber)
                _tablePtr = new PageHandlePtr(_pageLocator.GetReadOnlyPage(_innerCopy.PageNumber), false); // We are in read mode, no intention to modify yet (therefore no copy wasted)

            var _table = _tablePtr.Value;
            Debug.Assert(_table.IsOverflow, "This must be an overflow page.");

            long nodesPerChunk = _innerCopy.NodesPerChunk;
            long chunkIdx = nodeName / nodesPerChunk;
            long nodeNameInChunk = nodeName % nodesPerChunk; // This is the relative node inside this chunk.
           
            // This cannot happen as we are allocating when we allocate the node itself.
            if (chunkIdx > _innerCopy.Items)
                return new PrefixTreeNodeLocationPtr { PageNumber = PrefixTree.Constants.InvalidPage, NodeOffset = 0 };

            long physicalPage = ((long*)_table.DataPointer)[chunkIdx];
            if (physicalPage == PrefixTree.Constants.InvalidPage)
                return new PrefixTreeNodeLocationPtr { PageNumber = PrefixTree.Constants.InvalidPage, NodeOffset = 0 };

            return new PrefixTreeNodeLocationPtr
            {
                PageNumber = physicalPage,
                NodeOffset = nodeNameInChunk // This is the relative location inside this chunk.
            };
        }


    }
}

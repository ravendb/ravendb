using Sparrow;
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

    public enum TranslationTableMapMode
    {
        Read,
        ReadOrAllocate
    }

    public unsafe class PrefixTreeTranslationTableMutableState
    {        
        private readonly LowLevelTransaction _tx;
        private readonly PrefixTreeRootMutableState _header;
        private PrefixTreeTranslationTableHeader _innerCopy;

        private bool _isModified;

        public PrefixTreeTranslationTableMutableState(LowLevelTransaction tx, PrefixTreeRootMutableState state)
        {
            Debug.Assert(tx != null);

            this._tx = tx;
            this._header = state;
            this._innerCopy = state.Pointer->TranslationTable;
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

        public void Initialize ( int chunkTreeDepth )
        {            
            _innerCopy.NodesPerChunk = (int)Math.Pow(2, chunkTreeDepth) - 1;
            _innerCopy.InnerNodesPerChunk = (int)Math.Pow(2, chunkTreeDepth - 1) - 1;
            _innerCopy.ChunkSize = _innerCopy.NodesPerChunk * sizeof(PrefixTree.Node);
            _innerCopy.Items = PrefixTree.Constants.TranslationTableInitialItems;

            // We will allocate at least two pages to force an overflow page.
            var table = _tx.AllocateOverflowPage((int) (_innerCopy.Items * sizeof(long)));
            Debug.Assert(table.IsOverflow); // Making sure we got an overflow page.
            _innerCopy.PageNumber = table.PageNumber;

            Memory.SetInline(table.DataPointer, 0xFF, table.OverflowSize);

            Debug.Assert(((long*)table.DataPointer)[0] == PrefixTree.Constants.InvalidPage); // Make sure we are writing invalid page in memory.
            Debug.Assert(((long*)table.DataPointer)[table.OverflowSize / sizeof(long) - 1] == PrefixTree.Constants.InvalidPage); // Make sure we are writing invalid page in memory.            

            IsModified = true;
        }

        public bool AreNodesInSameChunk( long na, long nb )
        {
            long naChunkIdx = na / _innerCopy.NodesPerChunk;
            long nbChunkIdx = nb / _innerCopy.NodesPerChunk;

            return naChunkIdx == nbChunkIdx;
        }

        public PageLocationPtr MapVirtualToPhysical(long nodeName, TranslationTableMapMode mode = TranslationTableMapMode.Read)
        {
            Debug.Assert(nodeName >= PrefixTree.Constants.RootNodeName);
            Debug.Assert(_tx.Flags == TransactionFlags.ReadWrite || (_tx.Flags == TransactionFlags.Read && mode == TranslationTableMapMode.Read), "Allocate mode for mapping outside of a write transaction is an invalid operation");

            if (nodeName < PrefixTree.Constants.RootNodeName) // This shouldnt happen at all. 
                throw new InvalidOperationException("Cannot map tombstones and invalid node names into physical locations.");

            if (_innerCopy.PageNumber == PrefixTree.Constants.InvalidPage) // This shouldnt happen at all. 
                throw new InvalidOperationException("Mapping table has not being initialized.");

            long chunkIdx = nodeName / _innerCopy.NodesPerChunk;
            long nodeNameInChunk = nodeName % _innerCopy.NodesPerChunk; // This is the relative node inside this chunk.

            Page table = _tx.GetPage(_innerCopy.PageNumber); // We are in read mode, no intention to modify yet (therefore no copy wasted) 
            Debug.Assert(table.IsOverflow, "This must be an overflow page.");                                    

            if ( chunkIdx > _innerCopy.Items)
            {
                if (mode == TranslationTableMapMode.Read)
                    throw new InvalidOperationException("Mapping cannot allocate trees outside of a write transaction");

                // We need to allocate a larger table and update the page.
                // Allocate page with one more overflow.
                // Copy the current table into the new page
                // Initialize with Constant.InvalidPage the rest of the table.             
                // Update the header with the new page for the table. 
                // Free the old page. 
                // Set the modified flag on. 

                throw new NotImplementedException();
            }

            long physicalPage = ((long*)table.DataPointer)[chunkIdx];
            if (physicalPage == PrefixTree.Constants.InvalidPage)
            {
                if (mode == TranslationTableMapMode.Read)
                    return new PageLocationPtr { PageNumber = PrefixTree.Constants.InvalidPage, Offset = 0 };

                // We need to allocate a new tree page and store the physical page number it here.
                var chunk = _tx.AllocateOverflowPage(sizeof(PrefixTreePageHeader), _innerCopy.ChunkSize).ToPrefixTreePage();
                chunk.Chunk = chunkIdx;
                chunk.RootNodeName = chunkIdx * _innerCopy.NodesPerChunk;

                if (nodeName == PrefixTree.Constants.RootNodeName)
                {
                    chunk.ParentNodeName = (int)PrefixTree.Constants.InvalidNodeName;
                    chunk.ParentChunk = PrefixTree.Constants.InvalidPage;
                }
                else
                {
                    long parentOfRootName = GetParentName(chunk.RootNodeName);
                    chunk.ParentNodeName = (int)(parentOfRootName % _innerCopy.NodesPerChunk);
                    chunk.ParentChunk = parentOfRootName / _innerCopy.NodesPerChunk;
                }

                // Update the physical page, therefore we need to get a copy that will be eventually committed. 
                table = _tx.ModifyPage(_innerCopy.PageNumber);
                ((long*)table.DataPointer)[chunkIdx] = chunk.PageNumber;

                // Set the modified flag on.    
                this.IsModified = true;

                physicalPage = chunk.PageNumber;               
            }            

            return new PageLocationPtr
            {
                PageNumber = physicalPage,
                Offset = PrefixTreePage.GetNodeOffset(nodeNameInChunk) // This is the relative location inside this chunk.
            };
        }

        internal long GetParentName(long nodeName)
        {
            if (nodeName == PrefixTree.Constants.RootNodeName)
                return PrefixTree.Constants.RootNodeName;

            long pageSize = _innerCopy.NodesPerChunk;

            long nodeNameInChunk = nodeName % _innerCopy.NodesPerChunk; // This is the relative node inside this chunk.
            if (nodeNameInChunk != 0)
            {
                var chunkIdx = nodeName / pageSize;
                var parentNodeNameInChunk = (int)Math.Floor((float)(nodeNameInChunk - 1) / 2);
                return parentNodeNameInChunk + chunkIdx * pageSize;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        internal long GetRightChildName(long nodeName)
        {
            long pageSize = _innerCopy.NodesPerChunk;

            var chunkIdx = nodeName / pageSize;
            long nodeNameInChunk = nodeName % _innerCopy.NodesPerChunk; // This is the relative node inside this chunk.
            if (nodeNameInChunk < _innerCopy.InnerNodesPerChunk)
            {
                long rightNodeNameInChunk = 2 * nodeNameInChunk + 2;
                return rightNodeNameInChunk + chunkIdx * pageSize;
            }
            else
            {
                long innerNodesPerChunk = _innerCopy.InnerNodesPerChunk;

                var rightNodeNameInChunk = nodeNameInChunk - innerNodesPerChunk;
                return (innerNodesPerChunk * chunkIdx + (2 * rightNodeNameInChunk + 2)) * pageSize;
            }
        }

        internal long GetLeftChildName(long nodeName)
        {
            long pageSize = _innerCopy.NodesPerChunk;

            var chunkIdx = nodeName / pageSize;
            long nodeNameInChunk = nodeName % _innerCopy.NodesPerChunk; // This is the relative node inside this chunk.
            if (nodeNameInChunk < _innerCopy.InnerNodesPerChunk)
            {
                long leftNodeNameInChunk = 2 * nodeNameInChunk + 1;
                return leftNodeNameInChunk + chunkIdx * pageSize;
            }
            else
            {
                long innerNodesPerChunk = _innerCopy.InnerNodesPerChunk;

                var leftNodeNameInChunk = nodeNameInChunk - innerNodesPerChunk;
                return (innerNodesPerChunk * chunkIdx + (2 * leftNodeNameInChunk + 1)) * pageSize;
            }
        }
    }
}

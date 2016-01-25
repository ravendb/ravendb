using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
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
            _innerCopy.ChunkSize = _innerCopy.NodesPerChunk * sizeof(PrefixTree.Node) + sizeof(PrefixTreePageHeader);
            _innerCopy.PageNumber = PrefixTree.Constants.InvalidPage;

            IsModified = true;
        }

        public PageLocationPtr MapVirtualToPhysical(long nodeName, TranslationTableMapMode mode = TranslationTableMapMode.Read)
        {
            Debug.Assert(_tx.Flags == TransactionFlags.ReadWrite || (_tx.Flags == TransactionFlags.Read && mode == TranslationTableMapMode.Read), "Allocate mode for mapping outside of a write transaction is an invalid operation");

            if (_innerCopy.PageNumber == PrefixTree.Constants.InvalidPage)
                return new PageLocationPtr { PageNumber = PrefixTree.Constants.InvalidPage, Offset = 0 };

            long chunkIdx = nodeName / _innerCopy.NodesPerChunk;
            long nodeNameInChunk = nodeName % _innerCopy.NodesPerChunk;
            
            var table = _tx.GetPage(_innerCopy.PageNumber);
            Debug.Assert(table.IsOverflow, "This must be an overflow page.");                                    

            if ( chunkIdx > table.OverflowSize / sizeof(long) )
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
                // Update the physical page 
                // Set the modified flag on.                

                throw new NotImplementedException();
            }            

            // TODO: Change this to something from PrefixTree only. 
            return new PageLocationPtr
            {
                PageNumber = physicalPage,
                Offset = PrefixTreePage.GetNodeOffset(nodeNameInChunk)
            };
        }
    }
}

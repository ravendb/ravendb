using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl;

namespace Voron.Data.Compact
{
    public unsafe class PrefixTreeTableMutableState
    {
        private readonly LowLevelTransaction _tx;
        private readonly PrefixTreeRootMutableState _header;
        private PrefixTreeTableHeader _innerCopy;

        private bool _isModified;

        public PrefixTreeTableMutableState(LowLevelTransaction tx, PrefixTreeRootMutableState state)
        {
            Debug.Assert(tx != null);

            this._tx = tx;
            this._header = state;
            this._innerCopy = state.Pointer->Table;
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
        /// The current capacity of the dictionary
        /// </summary>
        public int Capacity
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _innerCopy.Capacity; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _innerCopy.Capacity = value;
                IsModified = true;
            }
        }

        /// <summary>
        /// This is the real counter of how many items are in the hash-table (regardless of buckets)
        /// </summary>
        public int Size
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _innerCopy.Size; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _innerCopy.Size = value;
                IsModified = true;
            }
        }

        /// <summary>
        /// How many used buckets. 
        /// </summary>
        public int NumberOfUsed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _innerCopy.NumberOfUsed; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _innerCopy.NumberOfUsed = value;
                IsModified = true;
            }
        }

        /// <summary>
        /// How many occupied buckets are marked deleted
        /// </summary>
        public int NumberOfDeleted
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _innerCopy.NumberOfDeleted; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _innerCopy.NumberOfDeleted = value;
                IsModified = true;
            }
        }

        public void Initialize()
        {
            _innerCopy = PrefixTree.InternalTable.Allocate(_tx, _header);
            IsModified = true;
        }

        /// <summary>
        /// The next growth threshold. 
        /// </summary>
        public int NextGrowthThreshold
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _innerCopy.NextGrowthThreshold; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _innerCopy.NextGrowthThreshold = value;
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

        public void CopyTo(PrefixTreeRootHeader* header)
        {
            header->Table = _innerCopy;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(PrefixTreeTableHeader newHeader)
        {
            _innerCopy = newHeader;

            _isModified = true;
        }
    }
}

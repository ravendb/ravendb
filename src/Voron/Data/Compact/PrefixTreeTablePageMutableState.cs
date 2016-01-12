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
    public unsafe class PrefixTreeTablePageMutableState
    {
        private readonly LowLevelTransaction _tx;
        private readonly PrefixTreeTablePageHeader* _header;

        private bool _isModified;

        public PrefixTreeTablePageMutableState(LowLevelTransaction tx, Page page)
        {
            Debug.Assert(tx != null);
            Debug.Assert(page != null);
            Debug.Assert(page.Pointer != null);

            this._tx = tx;
            this._header = (PrefixTreeTablePageHeader*)page.Pointer;

            this.PageNumber = page.PageNumber;
        }

        public long PageNumber { get; private set; }

        /// <summary>
        /// The current capacity of the dictionary
        /// </summary>
        public int Capacity
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Capacity; }
        }

        /// <summary>
        /// This is the real counter of how many items are in the hash-table (regardless of buckets)
        /// </summary>
        public int Size
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Size; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _header->Size = value;
                IsModified = true;
            }
        }

        /// <summary>
        /// How many used buckets. 
        /// </summary>
        public int NumberOfUsed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->NumberOfUsed; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _header->NumberOfUsed = value;
                IsModified = true;
            }
        }

        /// <summary>
        /// How many occupied buckets are marked deleted
        /// </summary>
        public int NumberOfDeleted
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->NumberOfDeleted; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _header->NumberOfDeleted = value;
                IsModified = true;
            }
        }

        /// <summary>
        /// The next growth threshold. 
        /// </summary>
        public int NextGrowthThreshold
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->NextGrowthThreshold; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _header->NextGrowthThreshold = value;
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
    }
}

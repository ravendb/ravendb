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
    public unsafe class PrefixTreeRootMutableState
    {
        private readonly LowLevelTransaction _tx;
        private readonly PrefixTreeRootHeader* _header;

        private bool _isModified;

        public PrefixTreeRootMutableState(LowLevelTransaction tx, PrefixTreeRootHeader* header)
        {
            Debug.Assert(tx != null);

            this._tx = tx;
            this._header = header;
        }

        /// <summary>
        /// The root header page for the tree. 
        /// </summary>
        public long RootPage
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->RootPage; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _header->RootPage = value;
                IsModified = true;
            }
        }

        /// <summary>
        /// The table header page for the tree.
        /// </summary>
        public long Table
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Table; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _header->Table = value;
                IsModified = true;
            }
        }

        /// <summary>
        /// The head node pointer for the tree. 
        /// </summary>
        public PrefixTree.Leaf Head
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Head; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _header->Head = value;
                IsModified = true;
            }
        }

        /// <summary>
        /// The tail node pointer for the tree. 
        /// </summary>
        public PrefixTree.Leaf Tail
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Tail; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _header->Tail = value;
                IsModified = true;
            }
        }

        /// <summary>
        /// This is the amount of elements already stored in the tree. 
        /// </summary>
        public long Items
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Items; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _header->Items = value;
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

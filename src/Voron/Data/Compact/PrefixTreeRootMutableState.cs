using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl;

namespace Voron.Data.Compact
{
    public unsafe class PrefixTreeRootMutableState : IDisposable
    {
        private readonly LowLevelTransaction _tx;
        private readonly PrefixTreeRootHeader* _pointer;

        private IntPtr _innerCopyPtr;
        private PrefixTreeRootHeader* _innerCopy;

        private readonly PrefixTreeTranslationTableMutableState _translationTable;
        private readonly PrefixTreeTableMutableState _table;

        private bool _isModified;

        public PrefixTreeRootMutableState(LowLevelTransaction tx, PrefixTreeRootHeader* header)
        {
            Debug.Assert(tx != null);
            Debug.Assert(header->RootObjectType == RootObjectType.None || header->RootObjectType == RootObjectType.PrefixTree);

            this._tx = tx;
            this._pointer = header;

            // Initialize unmanaged memory to hold the struct.
            _innerCopyPtr = Marshal.AllocHGlobal(sizeof(PrefixTreeRootHeader));
            _innerCopy = (PrefixTreeRootHeader*) _innerCopyPtr.ToPointer();
            Memory.CopyInline((byte*)_innerCopy, (byte*)header, sizeof(PrefixTreeRootHeader));

            this._innerCopy->RootObjectType = RootObjectType.PrefixTree;

            this._translationTable = new PrefixTreeTranslationTableMutableState(tx, this);
            this._table = new PrefixTreeTableMutableState(tx, this);
        }

        public PrefixTreeTranslationTableMutableState TranslationTable
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _translationTable; }
        }

        public PrefixTreeTableMutableState Table
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _table; }
        }

        public PrefixTreeRootHeader* Pointer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _innerCopy; }
        }

        /// <summary>
        /// The root node name for the tree. 
        /// </summary>
        public long RootNodeName
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _innerCopy->RootNodeName; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _innerCopy->RootNodeName = value;
                IsModified = true;
            }
        }

        /// <summary>
        /// The head node pointer for the tree. 
        /// </summary>
        public PrefixTree.Leaf Head
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _innerCopy->Head; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _innerCopy->Head = value;
                IsModified = true;
            }
        }

        /// <summary>
        /// The tail node pointer for the tree. 
        /// </summary>
        public PrefixTree.Leaf Tail
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _innerCopy->Tail; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _innerCopy->Tail = value;
                IsModified = true;
            }
        }

        /// <summary>
        /// This is the amount of elements already stored in the tree. 
        /// </summary>
        public long Items
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _innerCopy->Items; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                _innerCopy->Items = value;
                IsModified = true;
            }
        }

        public bool IsModified
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _isModified || _translationTable.IsModified; }
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
            Memory.CopyInline((byte*)header, (byte*)_innerCopy, sizeof(PrefixTreeRootHeader));
            if (_translationTable.IsModified)
                _translationTable.CopyTo(header);
            if (_table.IsModified)
                _table.CopyTo(header);
        }

        #region IDisposable Support

        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                if (_innerCopyPtr != IntPtr.Zero)
                    Marshal.FreeHGlobal(_innerCopyPtr);

                _innerCopyPtr = IntPtr.Zero;
                _innerCopy = null;

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        ~PrefixTreeRootMutableState()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(false);
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}

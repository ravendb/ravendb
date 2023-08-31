using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Utils.Enumerators
{
    public sealed class PulsedTransactionEnumerator<T, TState> : IEnumerator<T> where TState : PulsedEnumerationState<T>
    {
        private readonly DocumentsOperationContext _context;
        private readonly Func<TState, IEnumerable<T>> _getEnumerable;
        private readonly Func<TState, IEnumerator<T>> _getEnumerator;

        private readonly TState _state;

        private IEnumerator<T> _innerEnumerator;

        public PulsedTransactionEnumerator(DocumentsOperationContext context, Func<TState, IEnumerable<T>> getEnumerable, TState state)
        {
            _context = context;
            _getEnumerable = getEnumerable;
            _state = state;
            _innerEnumerator = _getEnumerable(state).GetEnumerator();
        }

        public PulsedTransactionEnumerator(DocumentsOperationContext context, Func<TState, IEnumerator<T>> getEnumerator, TState state)
        {
            _context = context;
            _getEnumerator = getEnumerator;
            _state = state;
            _innerEnumerator = _getEnumerator(state);
        }

        public bool MoveNext()
        {
            if (_state.ShouldPulseTransaction())
            {
                Debug.Assert(_context.Transaction.InnerTransaction.IsWriteTransaction == false, $"{nameof(PulsedTransactionEnumerator<T, TState>)} is meant to be used with read transactions only");

                _context.CloneReadTransaction();

                _innerEnumerator = _getEnumerator != null ? _getEnumerator(_state) : _getEnumerable(_state).GetEnumerator();
            }

            if (_innerEnumerator.MoveNext() == false)
                return false;
            
            Current = _innerEnumerator.Current;

            _state.OnMoveNext(Current);

            return true;
        }

        public void Reset()
        {
            throw new System.NotImplementedException();
        }

        public T Current { get; private set; }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }
    }
}

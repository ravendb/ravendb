using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Threading;
using Sparrow.Logging;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Sparrow;
using Sparrow.Threading;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexSearcherHolder : IDisposable
    {
        private readonly Func<IState, IndexSearcher> _recreateSearcher;
        private readonly DocumentDatabase _documentDatabase;

        private readonly Logger _logger;
        private ImmutableList<IndexSearcherHoldingState> _states = ImmutableList<IndexSearcherHoldingState>.Empty;

        public IndexSearcherHolder(Func<IState, IndexSearcher> recreateSearcher, DocumentDatabase documentDatabase)
        {
            _recreateSearcher = recreateSearcher;
            _documentDatabase = documentDatabase;
            _logger = LoggingSource.Instance.GetLogger<IndexSearcherHolder>(documentDatabase.Name);
        }

        public void SetIndexSearcher(Transaction asOfTx)
        {
            var state = new IndexSearcherHoldingState(asOfTx, _recreateSearcher, _documentDatabase.Name);

            _states = _states.Insert(0, state);

            Cleanup(asOfTx.LowLevelTransaction.Environment.PossibleOldestReadTransaction(asOfTx.LowLevelTransaction));
        }

        public IDisposable GetSearcher(Transaction tx, IState state, out IndexSearcher searcher)
        {
            var indexSearcherHoldingState = GetStateHolder(tx);
            try
            {
                searcher = indexSearcherHoldingState.GetIndexSearcher(state);
                return indexSearcherHoldingState;
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Failed to get the index searcher.", e);
                indexSearcherHoldingState.Dispose();
                throw;
            }
        }

        private IndexSearcherHoldingState GetStateHolder(Transaction tx)
        {
            var txId = tx.LowLevelTransaction.Id;

            foreach (var state in _states)
            {
                if (state.AsOfTxId > txId)
                {
                    continue;
                }

                return state;
            }

            throw new InvalidOperationException($"Could not get an index searcher state holder for transaction {txId}");
        }


        public void Cleanup(long oldestTx, IndexCleanup mode = IndexCleanup.Basic)
        {
            // note: cleanup cannot be called concurrently

            if (_states.Count > 1)
            {
                // let's mark states which are no longer needed as ready for disposal

                for (var i = _states.Count - 1; i >= 1; i--)
                {
                    var state = _states[i];

                    if (state.AsOfTxId >= oldestTx)
                        break;

                    var nextState = _states[i - 1];

                    if (nextState.AsOfTxId > oldestTx)
                        break;

                    Interlocked.Increment(ref state.Usage);

                    using (state)
                    {
                        state.MarkForDisposal();
                    }

                    _states = _states.Remove(state);
                }
            }

            if (mode.HasFlag(IndexCleanup.Readers))
            {
                foreach (var state in _states)
                {
                    state.MoveBackToLazy();
                }
            }
        }

        public void Dispose()
        {
            foreach (var state in _states)
            {
                using (state)
                {
                    state.MarkForDisposal();
                }
            }
        }

        internal class IndexSearcherHoldingState : IDisposable
        {
            private readonly Func<IState, IndexSearcher> _recreateSearcher;
            private readonly Logger _logger;
            private readonly MultipleUseFlag _isMovingToLazy = new MultipleUseFlag();

            private IState _indexSearcherInitializationState;
            private Lazy<IndexSearcher> _lazyIndexSearcher;

            public SingleUseFlag ShouldDispose = new SingleUseFlag();
            public int Usage;
            public readonly long AsOfTxId;

            public IndexSearcherHoldingState(Transaction tx, Func<IState, IndexSearcher> recreateSearcher, string dbName)
            {
                _recreateSearcher = recreateSearcher;
                _logger = LoggingSource.Instance.GetLogger<IndexSearcherHolder>(dbName);
                AsOfTxId = tx.LowLevelTransaction.Id;
                _lazyIndexSearcher = new Lazy<IndexSearcher>(() =>
                {
                    Debug.Assert(_indexSearcherInitializationState != null);
                    return _recreateSearcher(_indexSearcherInitializationState);
                });
            }

            public IndexSearcher GetIndexSearcher(IState state)
            {
                Interlocked.Increment(ref Usage);

                if (_isMovingToLazy.IsRaised())
                {
                    lock (this)
                    {
                        _indexSearcherInitializationState = state;
                        return _lazyIndexSearcher.Value;
                    }
                }

                _indexSearcherInitializationState = state;
                return _lazyIndexSearcher.Value;
            }

            ~IndexSearcherHoldingState()
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"IndexSearcherHoldingState wasn't properly disposed. Usage count: {Usage}, tx id: {AsOfTxId}, should dispose: {ShouldDispose.IsRaised()}");

                Dispose();
            }


            public void MoveBackToLazy()
            {
                var old = _lazyIndexSearcher;

                if (old.IsValueCreated == false)
                    return;

                lock (this)
                {
                    if (old != _lazyIndexSearcher)
                        return;
                    
                    _isMovingToLazy.Raise();

                    try
                    {
                        if (Volatile.Read(ref Usage) > 0)
                            return;

                        _lazyIndexSearcher = new Lazy<IndexSearcher>(() =>
                        {
                            Debug.Assert(_indexSearcherInitializationState != null);
                            return _recreateSearcher(_indexSearcherInitializationState);
                        }); 
                    }
                    finally
                    {
                        _isMovingToLazy.Lower();
                    }
                }

                using (old.Value)
                using (old.Value.IndexReader)
                { }
            }

            public void MarkForDisposal()
            {
                ShouldDispose.Raise();
            }

            public void Dispose()
            {
                if (Interlocked.Decrement(ref Usage) > 0)
                    return;

                if (ShouldDispose == false)
                    return;

                if (_lazyIndexSearcher.IsValueCreated)
                {
                    using (_lazyIndexSearcher.Value)
                    using (_lazyIndexSearcher.Value.IndexReader)
                    { }
                }

                GC.SuppressFinalize(this);
            }
        }

        public class StringCollectionValue
        {
            private readonly int _hashCode;
            private readonly uint _hash;
#if DEBUG
            // ReSharper disable once NotAccessedField.Local
            private List<string> _values;
#endif

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;
                if (ReferenceEquals(this, obj))
                    return true;
                var other = obj as StringCollectionValue;
                if (other == null)
                    return false;

                return _hash == other._hash;
            }

            public override int GetHashCode()
            {
                return _hashCode;
            }

            public unsafe StringCollectionValue(List<string> values)
            {
#if DEBUG
                _values = values;
#endif
                if (values.Count == 0)
                    ThrowEmptyFacets();

                _hashCode = values.Count;
                _hash = (uint)values.Count;

                _hash = 0;
                foreach (var value in values)
                {
                    fixed (char* p = value)
                    {
                        _hash = Hashing.XXHash32.Calculate((byte*)p, sizeof(char) * value.Length, _hash);
                    }
                    _hashCode = _hashCode * 397 ^ value.GetHashCode();
                }
            }

            private static void ThrowEmptyFacets()
            {
                throw new InvalidOperationException(
                    "Cannot apply distinct facet on empty fields, did you forget to store them in the index? ");
            }
        }
    }
}

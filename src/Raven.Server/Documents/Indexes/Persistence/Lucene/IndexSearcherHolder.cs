using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Sparrow.Logging;
using Lucene.Net.Search;
using Sparrow;
using Sparrow.Json;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexSearcherHolder
    {
        private readonly Func<IndexSearcher> _recreateSearcher;

        private readonly Logger _logger;
        private readonly LinkedList<IndexSearcherHoldingState> _states = new LinkedList<IndexSearcherHoldingState>();
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

        public IndexSearcherHolder(Func<IndexSearcher> recreateSearcher, DocumentDatabase documentDatabase)
        {
            _recreateSearcher = recreateSearcher;
            _logger = LoggingSource.Instance.GetLogger<IndexSearcherHolder>(documentDatabase.Name);
        }

        public void SetIndexSearcher(Transaction asOfTx)
        {
            var oldestTx = asOfTx.LowLevelTransaction.Environment.ActiveTransactions.OldestTransaction;
            var current = new IndexSearcherHoldingState(asOfTx, _recreateSearcher);

            _lock.EnterWriteLock();
            try
            {
                var newNode = _states.AddFirst(current);
                current.RemoveOnDispose = new RemoveState(this, newNode);

                Cleanup(oldestTx);
            }
            finally
            {
                _lock.ExitWriteLock();
            }
        }
        
        public IDisposable GetSearcher(Transaction tx, out IndexSearcher searcher)
        {
            var indexSearcherHoldingState = GetStateHolder(tx);
            try
            {
                searcher = indexSearcherHoldingState.IndexSearcher.Value;
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

        internal IndexSearcherHoldingState GetStateHolder(Transaction tx)
        {
            var txId = tx.LowLevelTransaction.Id;

            _lock.EnterReadLock();
            try
            {
                var current = _states.First;

                while (current != null)
                {
                    var state = current.Value;

                    if (state.AsOfTxId > txId)
                    {
                        current = current.Next;
                        continue;
                    }

                    Interlocked.Increment(ref state.Usage);

                    return state;
                }

                throw new InvalidOperationException($"Could not get an index searcher state holder for transaction {txId}");
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }

        public void Cleanup(long oldestTx)
        {
            if (_states.Count == 0)
                return;

            var lockTaken = false;
            if (_lock.IsWriteLockHeld == false)
            {
                _lock.EnterWriteLock();
                lockTaken = true;
            }

            try
            {
                if (oldestTx == 0) // no active transaction let's dispose and remove all except the latest one
                {
                    var toRemove = _states.First.Next;

                    while (toRemove != null)
                    {
                        using (toRemove.Value)
                        {
                            toRemove.Value.MarkForDisposal();
                        }

                        toRemove = toRemove.Next;
                    }

                    return;
                }

                // let's mark states which aren't be necessary as ready for disposal

                var latest = _states.First;
                var item = _states.Last;

                while (item != null && item != latest)
                {
                    var existingState = item.Value;

                    if (existingState.AsOfTxId >= oldestTx)
                        break;

                    var previous = item.Previous;

                    if (previous.Value.AsOfTxId > oldestTx)
                        break;

                    Interlocked.Increment(ref existingState.Usage);

                    using (existingState)
                    {
                        existingState.MarkForDisposal();
                    }

                    item = previous;
                }
            }
            finally
            {
                if (lockTaken)
                    _lock.ExitWriteLock();
            }
        }

        internal class RemoveState : IDisposable
        {
            private readonly IndexSearcherHolder _holder;
            private readonly LinkedListNode<IndexSearcherHoldingState> _node;

            public RemoveState(IndexSearcherHolder holder, LinkedListNode<IndexSearcherHoldingState> node)
            {
                _holder = holder;
                _node = node;
            }

            public void Dispose()
            {
                var lockTaken = false;

                if (_holder._lock.IsWriteLockHeld == false)
                {
                    lockTaken = true;
                    _holder._lock.EnterWriteLock();
                }

                try
                {
                    _holder._states.Remove(_node);
                }
                finally
                {
                    if (lockTaken)
                        _holder._lock.ExitWriteLock();
                }
            }
        }

        internal class IndexSearcherHoldingState : IDisposable
        {
            public readonly Lazy<IndexSearcher> IndexSearcher;

            public volatile bool ShouldDispose;
            public int Usage;
            public readonly long AsOfTxId;
            private readonly ConcurrentDictionary<Tuple<int, uint>, StringCollectionValue> _docsCache = new ConcurrentDictionary<Tuple<int, uint>, StringCollectionValue>();

            public IndexSearcherHoldingState(Transaction tx, Func<IndexSearcher> recreateSearcher)
            {
                IndexSearcher = new Lazy<IndexSearcher>(recreateSearcher, LazyThreadSafetyMode.ExecutionAndPublication);
                AsOfTxId = tx.LowLevelTransaction.Id;
            }

            public LinkedListNode<IndexSearcherHoldingState> Node;
            public RemoveState RemoveOnDispose { get; set; }

            public void MarkForDisposal()
            {
                ShouldDispose = true;
            }

            public void Dispose()
            {
                if (Interlocked.Decrement(ref Usage) > 0)
                    return;
                if (ShouldDispose == false)
                    return;
                DisposeRudely();
            }

            public StringCollectionValue GetFieldsValues(int docId, uint fieldsHash, string[] fields, JsonOperationContext context)
            {
                var key = Tuple.Create(docId, fieldsHash);

                StringCollectionValue value;
                if (_docsCache.TryGetValue(key, out value))
                    return value;

                return _docsCache.GetOrAdd(key, _ =>
                {
                    var doc = IndexSearcher.Value.Doc(docId);
                    return new StringCollectionValue((from field in fields
                                                      from fld in doc.GetFields(field)
                                                      where fld.StringValue != null
                                                      select fld.StringValue).ToList(), context);
                });

            }

            private void DisposeRudely()
            {
                if (IndexSearcher.IsValueCreated)
                {
                    using (IndexSearcher.Value)
                    using (IndexSearcher.Value.IndexReader) { }
                }

                RemoveOnDispose.Dispose();
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
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                var other = obj as StringCollectionValue;
                if (other == null) return false;

                return _hash == other._hash;
            }

            public override int GetHashCode()
            {
                return _hashCode;
            }

            public unsafe StringCollectionValue(List<string> values, JsonOperationContext context)
            {
#if DEBUG
                _values = values;
#endif
                if (values.Count == 0)
                    throw new InvalidOperationException("Cannot apply distinct facet on empty fields, did you forget to store them in the index? ");

                _hashCode = values.Count;
                _hash = (uint)values.Count;

                var size = values.Sum(x => x.Length);
                var buffer = context.GetNativeTempBuffer(size);
                var destChars = (char*)buffer;

                var position = 0;
                foreach (var value in values)
                {
                    for (var i = 0; i < value.Length; i++)
                        destChars[position++] = value[i];

                    unchecked
                    {
                        _hashCode = _hashCode * 397 ^ value.GetHashCode();
                    }
                }

                _hash = Hashing.XXHash32.Calculate(buffer, size);
            }
        }
    }
}
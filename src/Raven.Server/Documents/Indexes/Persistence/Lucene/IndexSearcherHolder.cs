using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Sparrow.Logging;
using Lucene.Net.Search;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class IndexSearcherHolder
    {
        private readonly Func<IndexSearcher> _recreateSearcher;

        private Logger _logger;
        private volatile IndexSearcherHoldingState _current;

        public IndexSearcherHolder(Func<IndexSearcher> recreateSearcher)
        {
            _recreateSearcher = recreateSearcher;
        }

        public ManualResetEvent SetIndexSearcher(bool wait)
        {
            var old = _current;
            _current = new IndexSearcherHoldingState(_recreateSearcher);

            if (old == null)
                return null;

            Interlocked.Increment(ref old.Usage);
            using (old)
            {
                if (wait)
                    return old.MarkForDisposalWithWait();
                old.MarkForDisposal();
                return null;
            }
        }

        public IDisposable GetSearcher(out IndexSearcher searcher, DocumentDatabase documentDatabase)
        {
            _logger = LoggingSource.Instance.GetLogger<IndexSearcherHolder>(documentDatabase.Name);
            var indexSearcherHoldingState = GetCurrentStateHolder();
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

        internal IndexSearcherHoldingState GetCurrentStateHolder()
        {
            while (true)
            {
                var state = _current;
                Interlocked.Increment(ref state.Usage);
                if (state.ShouldDispose)
                {
                    state.Dispose();
                    continue;
                }

                return state;
            }
        }


        internal class IndexSearcherHoldingState : IDisposable
        {
            public readonly Lazy<IndexSearcher> IndexSearcher;

            public volatile bool ShouldDispose;
            public int Usage;
            private readonly Lazy<ManualResetEvent> _disposed = new Lazy<ManualResetEvent>(() => new ManualResetEvent(false));
            private readonly ConcurrentDictionary<Tuple<int, uint>, StringCollectionValue> _docsCache = new ConcurrentDictionary<Tuple<int, uint>, StringCollectionValue>();

            public IndexSearcherHoldingState(Func<IndexSearcher> recreateSearcher)
            {
                IndexSearcher = new Lazy<IndexSearcher>(recreateSearcher, LazyThreadSafetyMode.ExecutionAndPublication);
            }

            public void MarkForDisposal()
            {
                ShouldDispose = true;
            }

            public ManualResetEvent MarkForDisposalWithWait()
            {
                var x = _disposed.Value;//  first create the value
                ShouldDispose = true;
                return x;
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
                if (_disposed.IsValueCreated)
                    _disposed.Value.Set();
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
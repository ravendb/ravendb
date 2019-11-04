using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Static
{
    public class StaticIndexDocsEnumerator<TType> : IIndexedItemEnumerator where TType : AbstractDynamicObject, new()
    {
        private readonly IndexingStatsScope _documentReadStats;
        private readonly IEnumerator<IndexingItem> _itemsEnumerator;
        private readonly IEnumerable _resultsOfCurrentDocument;
        private readonly MultipleIndexingFunctionsEnumerator<TType> _multipleIndexingFunctionsEnumerator;

        protected StaticIndexDocsEnumerator(IEnumerable<IndexingItem> items)
        {
            _itemsEnumerator = items.GetEnumerator();
        }

        public StaticIndexDocsEnumerator(IEnumerable<IndexingItem> items, List<IndexingFunc> funcs, string collection, IndexingStatsScope stats, IndexType type)
            : this(items)
        {
            _documentReadStats = stats?.For(IndexingOperation.Map.DocumentRead, start: false);

            var indexingFunctionType = type.IsJavaScript() ? IndexingOperation.Map.Jint : IndexingOperation.Map.Linq;

            var mapFuncStats = stats?.For(indexingFunctionType, start: false);

            if (funcs.Count == 1)
            {
                _resultsOfCurrentDocument =
                    new TimeCountingEnumerable(funcs[0](new DynamicIteratorOfCurrentDocumentWrapper<TType>(this)), mapFuncStats);
            }
            else
            {
                _multipleIndexingFunctionsEnumerator = new MultipleIndexingFunctionsEnumerator<TType>(funcs, new DynamicIteratorOfCurrentDocumentWrapper<TType>(this));
                _resultsOfCurrentDocument = new TimeCountingEnumerable(_multipleIndexingFunctionsEnumerator, mapFuncStats);
            }

            CurrentIndexingScope.Current.SetSourceCollection(collection, mapFuncStats);
        }

        public bool MoveNext(out IEnumerable resultsOfCurrentDocument)
        {
            using (_documentReadStats?.Start())
            {
                if (Current.Item is IDisposable disposable)
                    disposable.Dispose();

                if (_itemsEnumerator.MoveNext() == false)
                {
                    Current = default;
                    resultsOfCurrentDocument = null;

                    return false;
                }

                Current = _itemsEnumerator.Current;
                resultsOfCurrentDocument = _resultsOfCurrentDocument;

                return true;
            }
        }

        public void OnError()
        {
            _multipleIndexingFunctionsEnumerator?.Reset();
        }

        public IndexingItem Current { get; private set; }

        public void Dispose()
        {
            _itemsEnumerator.Dispose();

            if (Current.Item is IDisposable disposable)
                disposable.Dispose();
        }

        protected class DynamicIteratorOfCurrentDocumentWrapper<TType> : IEnumerable<TType> where TType : AbstractDynamicObject, new()
        {
            private readonly StaticIndexDocsEnumerator<TType> _indexingEnumerator;
            private Enumerator<TType> _enumerator;

            public DynamicIteratorOfCurrentDocumentWrapper(StaticIndexDocsEnumerator<TType> indexingEnumerator)
            {
                _indexingEnumerator = indexingEnumerator;
            }

            public IEnumerator<TType> GetEnumerator()
            {
                return _enumerator ?? (_enumerator = new Enumerator<TType>(_indexingEnumerator));
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private class Enumerator<TType> : IEnumerator<TType> where TType : AbstractDynamicObject, new()
            {
                private TType _dynamicDocument;
                private readonly StaticIndexDocsEnumerator<TType> _inner;
                private object _seen;

                public Enumerator(StaticIndexDocsEnumerator<TType> indexingEnumerator)
                {
                    _inner = indexingEnumerator;
                }

                public bool MoveNext()
                {
                    if (_seen == _inner.Current.Item) // already iterated
                        return false;

                    _seen = _inner.Current.Item;

                    if (_dynamicDocument == null)
                        _dynamicDocument = new TType();


                    _dynamicDocument.Set(_seen);

                    Current = _dynamicDocument;

                    CurrentIndexingScope.Current.Source = _dynamicDocument;

                    return true;
                }

                public void Reset()
                {
                    throw new NotSupportedException();
                }

                public TType Current { get; private set; }

                object IEnumerator.Current => Current;

                public void Dispose()
                {
                }
            }
        }

        private class MultipleIndexingFunctionsEnumerator<TType> : IEnumerable where TType : AbstractDynamicObject, new()
        {
            private readonly Enumerator<TType> _enumerator;

            public MultipleIndexingFunctionsEnumerator(List<IndexingFunc> funcs, DynamicIteratorOfCurrentDocumentWrapper<TType> iterationOfCurrentDocument)
            {
                _enumerator = new Enumerator<TType>(funcs, iterationOfCurrentDocument.GetEnumerator());
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return _enumerator;
            }

            public void Reset()
            {
                _enumerator.Reset();
            }

            private class Enumerator<TType> : IEnumerator where TType : AbstractDynamicObject
            {
                private readonly List<IndexingFunc> _funcs;
                private readonly IEnumerator<TType> _docEnumerator;
                private readonly TType[] _currentDoc = new TType[1];
                private int _index;
                private bool _moveNextDoc = true;
                private IEnumerator _currentFuncEnumerator;

                public Enumerator(List<IndexingFunc> funcs, IEnumerator<TType> docEnumerator)
                {
                    _funcs = funcs;
                    _docEnumerator = docEnumerator;
                }

                public bool MoveNext()
                {
                    if (_moveNextDoc && _docEnumerator.MoveNext() == false)
                        return false;

                    _moveNextDoc = false;

                    while (true)
                    {
                        if (_currentFuncEnumerator == null)
                        {
                            _currentDoc[0] = _docEnumerator.Current;
                            _currentFuncEnumerator = _funcs[_index](_currentDoc).GetEnumerator();
                        }

                        if (_currentFuncEnumerator.MoveNext() == false)
                        {
                            _currentFuncEnumerator = null;
                            _index++;

                            if (_index < _funcs.Count)
                                continue;

                            _index = 0;
                            _moveNextDoc = true;

                            return false;
                        }

                        Current = _currentFuncEnumerator.Current;
                        return true;
                    }
                }

                public void Reset()
                {
                    _index = 0;
                    _moveNextDoc = true;
                    _currentFuncEnumerator = null;
                }

                public object Current { get; private set; }
            }
        }
    }
}

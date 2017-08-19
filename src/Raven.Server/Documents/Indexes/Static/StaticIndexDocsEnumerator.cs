using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Static
{
    public class StaticIndexDocsEnumerator : IIndexedDocumentsEnumerator
    {
        private readonly IndexingStatsScope _documentReadStats;
        private readonly IEnumerator<Document> _docsEnumerator;
        protected IEnumerable ResultsOfCurrentDocument;
        private readonly MultipleIndexingFunctionsEnumerator _multipleIndexingFunctionsEnumerator;

        protected StaticIndexDocsEnumerator(IEnumerable<Document> docs)
        {
            _docsEnumerator = docs.GetEnumerator();
        }

        public StaticIndexDocsEnumerator(IEnumerable<Document> docs, List<IndexingFunc> funcs, string collection, IndexingStatsScope stats)
            : this(docs)
        {
            _documentReadStats = stats?.For(IndexingOperation.Map.DocumentRead, start: false);

            var linqStats = stats?.For(IndexingOperation.Map.Linq, start: false);

            if (funcs.Count == 1)
            {
                ResultsOfCurrentDocument =
                    new TimeCountingEnumerable(funcs[0](new DynamicIteratonOfCurrentDocumentWrapper(this)), linqStats);
            }
            else
            {
                _multipleIndexingFunctionsEnumerator = new MultipleIndexingFunctionsEnumerator(funcs, new DynamicIteratonOfCurrentDocumentWrapper(this));
                ResultsOfCurrentDocument = new TimeCountingEnumerable(_multipleIndexingFunctionsEnumerator, linqStats);
            }

            CurrentIndexingScope.Current.SetSourceCollection(collection, linqStats);
        }

        public bool MoveNext(out IEnumerable resultsOfCurrentDocument)
        {
            using (_documentReadStats?.Start())
            {
                Current?.Data.Dispose();

                if (_docsEnumerator.MoveNext() == false)
                {
                    Current = null;
                    resultsOfCurrentDocument = null;

                    return false;
                }

                Current = _docsEnumerator.Current;
                resultsOfCurrentDocument = ResultsOfCurrentDocument;

                return true;
            }
        }

        public void OnError()
        {
            _multipleIndexingFunctionsEnumerator?.Reset();
        }

        public Document Current { get; private set; }

        public void Dispose()
        {
            _docsEnumerator.Dispose();
            Current?.Data?.Dispose();
        }

        protected class DynamicIteratonOfCurrentDocumentWrapper : IEnumerable<DynamicBlittableJson>
        {
            private readonly StaticIndexDocsEnumerator _indexingEnumerator;
            private Enumerator _enumerator;

            public DynamicIteratonOfCurrentDocumentWrapper(StaticIndexDocsEnumerator indexingEnumerator)
            {
                _indexingEnumerator = indexingEnumerator;
            }

            public IEnumerator<DynamicBlittableJson> GetEnumerator()
            {
                return _enumerator ?? (_enumerator = new Enumerator(_indexingEnumerator));
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private class Enumerator : IEnumerator<DynamicBlittableJson>
            {
                private DynamicBlittableJson _dynamicDocument;
                private readonly StaticIndexDocsEnumerator _inner;
                private Document _seen;

                public Enumerator(StaticIndexDocsEnumerator indexingEnumerator)
                {
                    _inner = indexingEnumerator;
                }

                public bool MoveNext()
                {
                    if (_seen == _inner.Current) // already iterated
                        return false;

                    _seen = _inner.Current;

                    if (_dynamicDocument == null)
                        _dynamicDocument = new DynamicBlittableJson(_seen);
                    else
                        _dynamicDocument.Set(_seen);

                    Current = _dynamicDocument;

                    CurrentIndexingScope.Current.Source = _dynamicDocument;

                    return true;
                }

                public void Reset()
                {
                    throw new NotSupportedException();
                }

                public DynamicBlittableJson Current { get; private set; }

                object IEnumerator.Current => Current;

                public void Dispose()
                {
                }
            }
        }

        private class MultipleIndexingFunctionsEnumerator : IEnumerable
        {
            private readonly Enumerator _enumerator;

            public MultipleIndexingFunctionsEnumerator(List<IndexingFunc> funcs, DynamicIteratonOfCurrentDocumentWrapper iterationOfCurrentDocument)
            {
                _enumerator = new Enumerator(funcs, iterationOfCurrentDocument.GetEnumerator());
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return _enumerator;
            }

            public void Reset()
            {
                _enumerator.Reset();
            }

            private class Enumerator : IEnumerator
            {
                private readonly List<IndexingFunc> _funcs;
                private readonly IEnumerator<DynamicBlittableJson> _docEnumerator;
                private readonly DynamicBlittableJson[] _currentDoc = new DynamicBlittableJson[1];
                private int _index;
                private bool _moveNextDoc = true;
                private IEnumerator _currentFuncEnumerator;

                public Enumerator(List<IndexingFunc> funcs, IEnumerator<DynamicBlittableJson> docEnumerator)
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

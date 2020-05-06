using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Static
{
    public class StaticIndexItemEnumerator<TType> : IIndexedItemEnumerator where TType : AbstractDynamicObject, new()
    {
        private readonly IndexingStatsScope _documentReadStats;
        private readonly IEnumerator<IndexItem> _itemsEnumerator;
        private readonly Dictionary<string, IEnumerable> _resultsOfCurrentDocument;
        private readonly Dictionary<string, MultipleIndexingFunctionsEnumerator<TType>> _multipleIndexingFunctionsEnumerator;
        private readonly bool _singleKey;
        private readonly string _firstKey;
        private readonly bool _allItems;
        private readonly string _allItemsKey;
        private readonly IIndexItemFilterBehavior _filter;

        public StaticIndexItemEnumerator(IEnumerable<IndexItem> items, IIndexItemFilterBehavior filter, Dictionary<string, List<IndexingFunc>> funcs, string collection, IndexingStatsScope stats, IndexType type)
        {
            _itemsEnumerator = items.GetEnumerator();
            _documentReadStats = stats?.For(IndexingOperation.Map.DocumentRead, start: false);

            var indexingFunctionType = type.IsJavaScript() ? IndexingOperation.Map.Jint : IndexingOperation.Map.Linq;

            var mapFuncStats = stats?.For(indexingFunctionType, start: false);

            _resultsOfCurrentDocument = new Dictionary<string, IEnumerable>(StringComparer.OrdinalIgnoreCase);
            _singleKey = funcs.Count == 1;
            foreach (var kvp in funcs)
            {
                if (_singleKey)
                    _firstKey = kvp.Key;

                if (_allItems == false)
                {
                    switch (kvp.Key)
                    {
                        case Constants.Documents.Collections.AllDocumentsCollection:
                            {
                                _allItems = true;
                                _allItemsKey = Constants.Documents.Collections.AllDocumentsCollection;
                                break;
                            }

                        case Constants.Counters.All:
                            {
                                _allItems = true;
                                _allItemsKey = Constants.Counters.All;
                                break;
                            }

                        case Constants.TimeSeries.All:
                            {
                                _allItems = true;
                                _allItemsKey = Constants.TimeSeries.All;
                                break;
                            }
                    }
                }

                if (kvp.Value.Count == 1)
                    _resultsOfCurrentDocument[kvp.Key] = new TimeCountingEnumerable(kvp.Value[0](new DynamicIteratorOfCurrentItemWrapper<TType>(this)), mapFuncStats);
                else
                {
                    if (_multipleIndexingFunctionsEnumerator == null)
                        _multipleIndexingFunctionsEnumerator = new Dictionary<string, MultipleIndexingFunctionsEnumerator<TType>>(StringComparer.OrdinalIgnoreCase);

                    var multipleIndexingFunctionsEnumerator = _multipleIndexingFunctionsEnumerator[kvp.Key] = new MultipleIndexingFunctionsEnumerator<TType>(kvp.Value, new DynamicIteratorOfCurrentItemWrapper<TType>(this));
                    _resultsOfCurrentDocument[kvp.Key] = new TimeCountingEnumerable(multipleIndexingFunctionsEnumerator, mapFuncStats);
                }
            }

            CurrentIndexingScope.Current.SetSourceCollection(collection, mapFuncStats);
            _filter = filter;
        }

        public bool MoveNext(out IEnumerable resultsOfCurrentDocument, out long? etag)
        {
            using (_documentReadStats?.Start())
            {
                Current?.Dispose();
                etag = null;

                while (_itemsEnumerator.MoveNext())
                {
                    Current = _itemsEnumerator.Current;
                    etag = Current.Etag;

                    if (_filter != null && _filter.ShouldFilter(Current))
                        continue;

                    if (Current.Empty)
                        resultsOfCurrentDocument = Enumerable.Empty<TType>();
                    else if (Current.IndexingKey == null && _singleKey)
                        resultsOfCurrentDocument = _resultsOfCurrentDocument[_firstKey];
                    else if (_allItems)
                        resultsOfCurrentDocument = _resultsOfCurrentDocument[_allItemsKey];
                    else if (_resultsOfCurrentDocument.TryGetValue(Current.IndexingKey, out resultsOfCurrentDocument) == false)
                        continue;

                    return true;
                }

                Current = default;
                resultsOfCurrentDocument = null;

                return false;
            }
        }

        public void OnError()
        {
            if (_multipleIndexingFunctionsEnumerator == null)
                return;

            if (_multipleIndexingFunctionsEnumerator.TryGetValue(Current.IndexingKey ?? _firstKey, out var func) == false)
                return;

            func?.Reset();
        }

        public IndexItem Current { get; private set; }

        public void Dispose()
        {
            _itemsEnumerator.Dispose();
            Current?.Dispose();
        }

        protected class DynamicIteratorOfCurrentItemWrapper<TDynamicIteratorOfCurrentItemWrapperType> : IEnumerable<TDynamicIteratorOfCurrentItemWrapperType> where TDynamicIteratorOfCurrentItemWrapperType : AbstractDynamicObject, new()
        {
            private readonly StaticIndexItemEnumerator<TDynamicIteratorOfCurrentItemWrapperType> _indexingEnumerator;
            private Enumerator<TDynamicIteratorOfCurrentItemWrapperType> _enumerator;

            public DynamicIteratorOfCurrentItemWrapper(StaticIndexItemEnumerator<TDynamicIteratorOfCurrentItemWrapperType> indexingEnumerator)
            {
                _indexingEnumerator = indexingEnumerator;
            }

            public IEnumerator<TDynamicIteratorOfCurrentItemWrapperType> GetEnumerator()
            {
                return _enumerator ??= new Enumerator<TDynamicIteratorOfCurrentItemWrapperType>(_indexingEnumerator);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private class Enumerator<TEnumeratorType> : IEnumerator<TEnumeratorType> where TEnumeratorType : AbstractDynamicObject, new()
            {
                private TEnumeratorType _dynamicItem;
                private readonly StaticIndexItemEnumerator<TEnumeratorType> _inner;
                private object _seen;

                public Enumerator(StaticIndexItemEnumerator<TEnumeratorType> indexingEnumerator)
                {
                    _inner = indexingEnumerator;
                }

                public bool MoveNext()
                {
                    if (_seen == _inner.Current.Item) // already iterated
                        return false;

                    _seen = _inner.Current.Item;

                    if (_dynamicItem == null)
                        _dynamicItem = new TEnumeratorType();

                    if (_dynamicItem.Set(_seen) == false)
                        return false;

                    Current = _dynamicItem;

                    CurrentIndexingScope.Current.Source = _dynamicItem;

                    return true;
                }

                public void Reset()
                {
                    throw new NotSupportedException();
                }

                public TEnumeratorType Current { get; private set; }

                object IEnumerator.Current => Current;

                public void Dispose()
                {
                }
            }
        }

        private class MultipleIndexingFunctionsEnumerator<TMultipleIndexingFunctionsEnumeratorType> : IEnumerable where TMultipleIndexingFunctionsEnumeratorType : AbstractDynamicObject, new()
        {
            private readonly Enumerator<TMultipleIndexingFunctionsEnumeratorType> _enumerator;

            public MultipleIndexingFunctionsEnumerator(List<IndexingFunc> funcs, DynamicIteratorOfCurrentItemWrapper<TMultipleIndexingFunctionsEnumeratorType> iterationOfCurrentDocument)
            {
                _enumerator = new Enumerator<TMultipleIndexingFunctionsEnumeratorType>(funcs, iterationOfCurrentDocument.GetEnumerator());
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return _enumerator;
            }

            public void Reset()
            {
                _enumerator.Reset();
            }

            private class Enumerator<TEnumeratorType> : IEnumerator where TEnumeratorType : AbstractDynamicObject
            {
                private readonly List<IndexingFunc> _funcs;
                private readonly IEnumerator<TEnumeratorType> _docEnumerator;
                private readonly TEnumeratorType[] _currentDoc = new TEnumeratorType[1];
                private int _index;
                private bool _moveNextDoc = true;
                private IEnumerator _currentFuncEnumerator;

                public Enumerator(List<IndexingFunc> funcs, IEnumerator<TEnumeratorType> docEnumerator)
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

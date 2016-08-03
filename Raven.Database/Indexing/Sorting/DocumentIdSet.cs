using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Lucene.Net.Index;
using Raven.Database.Config;
using Raven.Abstractions.Data;
using Sparrow.Collections;

namespace Raven.Database.Indexing.Sorting
{
    public static class DocumentIdSet
    {
        public class DocumentIdSetCleaner : ILowMemoryHandler
        {
            public LowMemoryHandlerStatistics HandleLowMemory()
            {
                var allReaders = _keys.Select(x =>
                {
                    IndexReader reader;
                    x.TryGetTarget(out reader);
                    return reader;

                }).Where(x => x != null).Distinct();
                var documentIdsSetsCount = allReaders.Sum(x =>
              {
                  var c = 0;
                  ConcurrentDictionary<Tuple<string, Predicate<string>>, Predicate<int>> documentIdsSets;
                  if (documentIdsSetBuildersCache.TryGetValue(x, out documentIdsSets))
                  {
                      c += documentIdsSets.Count;
                  }
                  return c;
              });

                var valuesInReadersCacheCount = allReaders.Sum(x =>
                {
                    var c = 0;
                    ConcurrentDictionary<string, Dictionary<int, string>> valuesInReadersCache;
                    if (fieldsStringValuesInReadersCache.TryGetValue(x, out valuesInReadersCache))
                    {
                        c += valuesInReadersCache.Count;
                    }
                    return c;
                });

                documentIdsSetBuildersCache = new ConditionalWeakTable<IndexReader, ConcurrentDictionary<Tuple<string, Predicate<string>>, Predicate<int>>>();

                fieldsStringValuesInReadersCache = new ConditionalWeakTable<IndexReader, ConcurrentDictionary<string, Dictionary<int, string>>>();
                _keys = new ConcurrentSet<WeakReference<IndexReader>>();

                return new LowMemoryHandlerStatistics
                {
                    Name = "DocumentIdSet",
                    DatabaseName = null,
                    Summary = $"Cleared {documentIdsSetsCount} Document Ids sets with {allReaders} readers " +
                             $"Cleared {valuesInReadersCacheCount} values in readers cache with {allReaders} readers"
                };
            }

            public LowMemoryHandlerStatistics GetStats()
            {
                var documentIdsSetBuildersCacheSize = _keys.Select(x =>
                {
                    IndexReader reader;
                    x.TryGetTarget(out reader);
                    return reader;
                })
                .Where(x => x != null)
                .Distinct().Select(x =>
                {
                    long curSize = 0;
                    ConcurrentDictionary<Tuple<string, Predicate<string>>, Predicate<int>> documentIdsSets;
                    if (documentIdsSetBuildersCache.TryGetValue(x, out documentIdsSets))
                    {
                        //SparseDocumentIdSet,List
                        foreach (var predicate in documentIdsSets)
                        {
                            curSize += predicate.Key.Item1.Length * sizeof(char);
                            var documentsIDsSetBuilder = predicate.Value.Target as DocumentsIDsSetBuilder;
                            if (documentsIDsSetBuilder != null)
                            {
                                curSize += documentsIDsSetBuilder.GetSize();
                            }
                            else
                            {
                                var sparseDocumentIdSet = predicate.Value.Target as SparseDocumentIdSet;
                                if (sparseDocumentIdSet != null)
                                    curSize = sparseDocumentIdSet.GetSize();
                            }
                        }
                    }

                    ConcurrentDictionary<string, Dictionary<int, string>> fieldStringValues;

                    if (fieldsStringValuesInReadersCache.TryGetValue(x, out fieldStringValues))
                    {
                        foreach (var fieldValuesPair in fieldStringValues)
                        {
                            curSize += fieldValuesPair.Key.Length * sizeof(char);
                            foreach (var docsToValuesKeyPair in fieldValuesPair.Value)
                            {
                                curSize += docsToValuesKeyPair.Key * sizeof(int);
                                curSize += docsToValuesKeyPair.Value.Length * sizeof(char);
                            }
                        }

                    }
                    return curSize;

                }).Sum();

                return new LowMemoryHandlerStatistics
                {
                    DatabaseName = null,
                    EstimatedUsedMemory = documentIdsSetBuildersCacheSize,
                    Name = "DocumentIdSet"
                };
            }
        }

        static DocumentIdSet()
        {
            MemoryStatistics.RegisterLowMemoryHandler(documentIdSetCleaner);
        }

        private static ConditionalWeakTable<IndexReader, ConcurrentDictionary<Tuple<string, Predicate<string>>, Predicate<int>>> documentIdsSetBuildersCache = new ConditionalWeakTable<IndexReader, ConcurrentDictionary<Tuple<string, Predicate<string>>, Predicate<int>>>();
        private static ConditionalWeakTable<IndexReader, ConcurrentDictionary<string, Dictionary<int, string>>> fieldsStringValuesInReadersCache = new ConditionalWeakTable<IndexReader, ConcurrentDictionary<string, Dictionary<int, string>>>();
        private readonly static DocumentIdSetCleaner documentIdSetCleaner = new DocumentIdSetCleaner();

        private static ConcurrentSet<WeakReference<IndexReader>> _keys = new ConcurrentSet<WeakReference<IndexReader>>();

        public static Predicate<int> GetDocsExistenceVerificationMethodInSet(this IndexReader reader, string field, Predicate<string> termAcceptanceFunction)
        {
            ConcurrentDictionary<Tuple<string, Predicate<string>>, Predicate<int>> value;
            if (documentIdsSetBuildersCache.TryGetValue(reader, out value) == false)
            {
                value = documentIdsSetBuildersCache.GetOrCreateValue(reader);
                _keys.Add(new WeakReference<IndexReader>(reader));
                IndexReader target;
                _keys.RemoveWhere(x => x.TryGetTarget(out target) == false);
            }

            var readerFieldTuple = Tuple.Create(field, termAcceptanceFunction);

            Predicate<int> predicate;
            if (value.TryGetValue(readerFieldTuple, out predicate))
                return predicate;


            var termDocs = reader.TermDocs();
            var termEnum = reader.Terms(new Term(field));
            int t = 0; // current term number
            var maxDoc = reader.MaxDoc;
            var documentsIDsSet = new DocumentsIDsSetBuilder(reader.MaxDoc);

            do
            {
                Term term = termEnum.Term;
                if (term == null || term.Field != field || t >= maxDoc) break;

                bool termMatches = termAcceptanceFunction(term.Text);

                if (termMatches)
                {
                    termDocs.Seek(termEnum);
                    while (termDocs.Next())
                    {
                        documentsIDsSet.Set(termDocs.Doc);
                    }
                }

                t++;
            }
            while (termEnum.Next());
            predicate = documentsIDsSet.Build();
            value.TryAdd(readerFieldTuple, predicate);
            return predicate;
        }

        public static Dictionary<int, string> GetDocsAndValuesDictionary(this IndexReader reader, string field)
        {
            ConcurrentDictionary<string, Dictionary<int, string>> docsValuesDictionary;
            if (fieldsStringValuesInReadersCache.TryGetValue(reader, out docsValuesDictionary) == false)
            {
                docsValuesDictionary = fieldsStringValuesInReadersCache.GetOrCreateValue(reader);
                _keys.Add(new WeakReference<IndexReader>(reader));
                IndexReader target;
                _keys.RemoveWhere(x => x.TryGetTarget(out target) == false);
            }
            Dictionary<int, string> value;
            if (docsValuesDictionary.TryGetValue(field, out value))
            {
                return value;
            }

            value = new Dictionary<int, string>();

            var termDocs = reader.TermDocs();
            var termEnum = reader.Terms(new Term(field));
            var t = 0; // current term number
            var maxDoc = reader.MaxDoc;

            do
            {
                var term = termEnum.Term;
                if (term == null || term.Field != field || t >= maxDoc) break;

                termDocs.Seek(termEnum);
                while (termDocs.Next())
                {
                    value[termDocs.Doc] = term.Text;
                }


                t++;
            }
            while (termEnum.Next());

            docsValuesDictionary.TryAdd(field, value);
            return value;
        }

        /// <summary>
        /// Used to calculate existence of fields in an index, where the field doesn't exists
        /// for most of the documents.
        /// 
        /// It has 1Kb overhead at all times, with a worst case scenario, takes NumberOfDocuments bits + 1Kb.
        /// 
        /// Given a 256K documents in an index, the worst case would be 33Kb set.
        /// 
        /// A more common scenario, where we have much fewer items in the set than the documents (let us say, 4K out of 256K), we 
        /// would take 1Kb + 2Kb only.
        /// </summary>
        public class SparseDocumentIdSet
        {
            private const int _numOfBitArrays = 128;
            private readonly BitArray[] _bitArrays = new BitArray[_numOfBitArrays];
            private readonly int _bitArraySize;

            public SparseDocumentIdSet(int size)
            {
                _bitArraySize = (int)Math.Ceiling((double)size / _numOfBitArrays);
            }

            public void Set(int docId)
            {
                var idx = docId / _bitArraySize;
                if (_bitArrays[idx] == null)
                    _bitArrays[idx] = new BitArray(_bitArraySize);

                _bitArrays[idx].Set(docId % _bitArraySize, true);
            }

            public bool Contains(int docId)
            {
                var idx = docId / _bitArraySize;
                if (_bitArrays[idx] == null)
                    return false;
                return _bitArrays[idx].Get(docId % _bitArraySize);

            }

            public int GetSize()
            {
                return _bitArrays.Sum(x =>
                {
                    if (x != null)
                        return x.Count / 8;
                    return 0;
                });
            }
        }

        public class DocumentsIDsSetBuilder
        {
            private readonly int _size;
            private List<int> _small = new List<int>(32);
            private SparseDocumentIdSet _large;

            public DocumentsIDsSetBuilder(int size)
            {
                this._size = size;
            }

            public void Set(int docId)
            {
                if (_large != null)
                    _large.Set(docId);
                if (_small.Count + 1 > 256)
                {
                    _large = new SparseDocumentIdSet(_size);
                    foreach (var doc in _small)
                    {
                        _large.Set(doc);
                    }
                    _large.Set(docId);
                    return;
                }
                _small.Add(docId);
            }

            public Predicate<int> Build()
            {
                if (_large != null)
                    return _large.Contains;
                _small.Sort();
                return i => _small.BinarySearch(i) >= 0;
            }

            public int GetSize()
            {
                if (_large != null)
                    return _large.GetSize();
                return 32 * sizeof(int);
            }
        }
    }
}

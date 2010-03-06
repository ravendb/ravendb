using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using log4net;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Storage;

namespace Raven.Database.Indexing
{
    /// <summary>
    /// This is a thread safe, single instance for a particular index.
    /// </summary>
    public class Index : IDisposable
    {
        private readonly ILog log = LogManager.GetLogger(typeof(Index));

        private class CurrentIndexSearcher
        {
            public IndexSearcher Searcher { get; set; }
            private int useCount;
            private bool shouldDisposeWhenThereAreNoUsages;


            public IDisposable Use()
            {
                Interlocked.Increment(ref useCount);
                return new CleanUp(this);
            }

            private class CleanUp : IDisposable
            {
                private readonly CurrentIndexSearcher parent;

                public CleanUp(CurrentIndexSearcher parent)
                {
                    this.parent = parent;
                }

                public void Dispose()
                {
                    var uses = Interlocked.Decrement(ref parent.useCount);
                    if (parent.shouldDisposeWhenThereAreNoUsages && uses == 0)
                        parent.Searcher.Close();

                }
            }

            public void MarkForDispoal()
            {
                shouldDisposeWhenThereAreNoUsages = true;
            }
        }

        private readonly Directory directory;
        private CurrentIndexSearcher searcher;
        private readonly string name;

        public Index(Directory directory, string name)
        {
            this.name = name;
            log.DebugFormat("Creating index for {0}", name);
            this.directory = directory;
            searcher = new CurrentIndexSearcher
            {
                Searcher = new IndexSearcher(directory)
            };
        }

        public void Dispose()
        {
            searcher.Searcher.Close();
            directory.Close();
        }

        public IEnumerable<string> Query(string query, int start, int pageSize, Reference<int> totalSize)
        {
            using (searcher.Use())
            {
                var indexSearcher = searcher.Searcher;
                if (string.IsNullOrEmpty(query) == false)
                {
                    return SearchIndex(query, indexSearcher, totalSize, start, pageSize);
                }
                return BrowseIndex(indexSearcher, totalSize, start, pageSize);
            }
        }

        private IEnumerable<string> BrowseIndex(IndexSearcher indexSearcher, Reference<int> totalSize, int start, int pageSize)
        {
            log.DebugFormat("Browsing index {0}", name);
            var maxDoc = indexSearcher.MaxDoc();
            totalSize.Value = maxDoc;
            for (int i = start; i < maxDoc && (i - start) < pageSize; i++)
            {
                yield return indexSearcher.Doc(i).GetField("__document_id").StringValue();
            }
        }

        private IEnumerable<string> SearchIndex(string query, IndexSearcher indexSearcher, Reference<int> totalSize, int start, int pageSize)
        {
            log.DebugFormat("Issuing query on index {0} for: {1}", name, query);
            var luceneQuery = new QueryParser("", new StandardAnalyzer()).Parse(query);
            var search = indexSearcher.Search(luceneQuery);
            totalSize.Value = search.Length();
            for (int i = start; i < search.Length() && (i - start) < pageSize; i++)
            {
                yield return search.Doc(i).GetField("__document_id").StringValue();
            }
        }

        private void Write(Func<IndexWriter, bool> action)
        {
            var indexWriter = new IndexWriter(directory, new StandardAnalyzer());
            bool shouldRcreateSearcher;
            try
            {
                shouldRcreateSearcher = action(indexWriter);
            }
            finally
            {
                indexWriter.Close();
            }
            if (shouldRcreateSearcher)
                RecreateSearcher();
        }

        public void IndexDocuments(IndexingFunc func, IEnumerable<object> documents, WorkContext context, DocumentStorageActions actions)
        {
            actions.SetCurrentIndexStatsTo(name);
            var count = 0;
            Write(indexWriter =>
            {
                var currentBatch = new List<Document>();
                string currentId = null;
                var converter = new JsonToLuceneDocumentConverter();
                var shouldRecreateIndex = false;
                foreach (var doc in RobustEnumeration(documents, func, actions, context))
                {
                    count++;
                    string newDocId;
                    var document = converter.Index(doc, out newDocId);

                    if(newDocId != currentId)
                    {
                        shouldRecreateIndex = FlushToIndex(indexWriter, currentId, currentBatch);
                        currentBatch.Clear();
                        currentId = newDocId;
                    }
                    currentBatch.Add(document);

                    actions.IncrementSuccessIndexing();

                }

                shouldRecreateIndex |= FlushToIndex(indexWriter, currentId, currentBatch);

                return shouldRecreateIndex;
            });
            log.InfoFormat("Indexed {0} documents for {1}", count, name);
        }

        private static bool FlushToIndex(IndexWriter indexWriter, string currentId, IEnumerable<Document> currentBatch)
        {
            if (currentId == null)
                return false;

            indexWriter.DeleteDocuments(new Term("__document_id", currentId));
            foreach (var document in currentBatch)
            {
                indexWriter.AddDocument(document);
            }
            return true;
        }

        private IEnumerable<object> RobustEnumeration(IEnumerable<object> input, IndexingFunc func, DocumentStorageActions actions, WorkContext context)
        {
            var wrapped = new StatefulEnumerableWrapper<dynamic>(input.GetEnumerator());
            var en = func(wrapped).GetEnumerator();
            do
            {
                var moveSuccessful = MoveNext(en, wrapped, context, actions);
                if (moveSuccessful == false)
                    yield break;
                if (moveSuccessful == true)
                    yield return en.Current;
                else
                    en = func(wrapped).GetEnumerator();
            } while (true);
        }

        private bool? MoveNext(IEnumerator en, StatefulEnumerableWrapper<object> innerEnumerator, WorkContext context, DocumentStorageActions actions)
        {
            try
            {
                actions.IncrementIndexingAttempt();
                var moveNext = en.MoveNext();
                if (moveNext == false)
                    actions.DecrementIndexingAttempt();
                return moveNext;
            }
            catch (Exception e)
            {
                actions.IncrementIndexingFailure();
                context.AddError(name,
                    TryGetDocKey(innerEnumerator.Current),
                    e.Message
                    );
                log.WarnFormat(e, "Failed to execute indexing function on {0} on {1}", name,
                    GetDocId(innerEnumerator));
            }
            return null;
        }

        private static string TryGetDocKey(object current)
        {
            var dic = current as IDictionary<string, object>;
            if (dic == null)
                return null;
            object value;
            dic.TryGetValue("__document_id", out value);
            if (value == null)
                return null;
            return value.ToString();
        }

        private static object GetDocId(StatefulEnumerableWrapper<object> currentInnerEnumerator)
        {
            var dictionary = currentInnerEnumerator.Current as IDictionary<string, object>;
            if (dictionary == null)
                return null;
            object docId;
            dictionary.TryGetValue("__document_id", out docId);
            return docId;
        }

        private void RecreateSearcher()
        {
            using (searcher.Use())
            {
                searcher.MarkForDispoal();
                searcher = new CurrentIndexSearcher
                {
                    Searcher = new IndexSearcher(directory)
                };
                Thread.MemoryBarrier();// force other threads to see this write
            }
        }

        public void Remove(string[] keys)
        {
            Write(writer =>
            {
                if (log.IsDebugEnabled)
                {
                    log.DebugFormat("Deleting ({0}) from {1}", string.Format(", ", keys), name);
                }
                writer.DeleteDocuments(keys.Select(k => new Term("__document_id", k)).ToArray());
                return true;
            });
        }
    }
}
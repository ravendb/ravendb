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
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Storage;

namespace Raven.Database.Indexing
{
    /// <summary>
    ///   This is a thread safe, single instance for a particular index.
    /// </summary>
    public abstract class Index : IDisposable
    {
        private readonly Directory directory;
        protected readonly ILog log = LogManager.GetLogger(typeof(Index));
        protected readonly string name;
        private CurrentIndexSearcher searcher;

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

        #region IDisposable Members

        public void Dispose()
        {
            searcher.Searcher.Close();
            directory.Close();
        }

        #endregion

        public IEnumerable<IndexQueryResult> Query(string query, int start, int pageSize, Reference<int> totalSize, string[] fieldsToFetch)
        {
            if (string.IsNullOrEmpty(query) == false)
            {
                return SearchIndex(query, totalSize, start, pageSize, fieldsToFetch);
            }
            return BrowseIndex(totalSize, start, pageSize, fieldsToFetch);
        }

        private IEnumerable<IndexQueryResult> BrowseIndex(Reference<int> totalSize, int start,
                                                int pageSize, string[] fieldsToFetch)
        {
            using (searcher.Use())
            {
                log.DebugFormat("Browsing index {0}", name);
                var indexReader = searcher.Searcher.Reader;
                var maxDoc = indexReader.MaxDoc();
                totalSize.Value = Enumerable.Range(0, maxDoc).Count(i => indexReader.IsDeleted(i) == false);
                var previousDocuments = new HashSet<string>();
                for (var i = start; i < maxDoc && (i - start) < pageSize; i++)
                {
                    if (indexReader.IsDeleted(i))
                        continue;
                    var document = indexReader.Document(i);
                    if (IsDuplicateDocument(document, fieldsToFetch, previousDocuments))
                        continue;
                    yield return RetrieveDocument(document, fieldsToFetch);
                }
            }
        }

        private static bool IsDuplicateDocument(Document document, string[] fieldsToFetch, HashSet<string> previousDocuments)
        {
            if (fieldsToFetch != null && fieldsToFetch.Length > 1)
                return false;
            return previousDocuments.Add(document.Get("__document_id")) == false;
        }

        public abstract void IndexDocuments(AbstractViewGenerator viewGenerator, IEnumerable<object> documents,
                                            WorkContext context,
                                            DocumentStorageActions actions);

        protected abstract IndexQueryResult RetrieveDocument(Document document, string[] fieldsToFetch);

        private IEnumerable<IndexQueryResult> SearchIndex(string query, Reference<int> totalSize,
                                                int start, int pageSize, string[] fieldsToFetch)
        {
           using(searcher.Use())
           {
               log.DebugFormat("Issuing query on index {0} for: {1}", name, query);
               var luceneQuery = new QueryParser("", new StandardAnalyzer()).Parse(query);
               var search = searcher.Searcher.Search(luceneQuery);
               totalSize.Value = search.Length();
               var previousDocuments = new HashSet<string>();
               for (var i = start; i < search.Length() && (i - start) < pageSize; i++)
               {
                   var document = search.Doc(i);
                   if (IsDuplicateDocument(document, fieldsToFetch, previousDocuments))
                       continue;
                   yield return RetrieveDocument(document, fieldsToFetch);
               }
           }
        }

        protected void Write(Func<IndexWriter, bool> action)
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


        protected IEnumerable<object> RobustEnumeration(IEnumerable<object> input, IndexingFunc func,
                                                      DocumentStorageActions actions, WorkContext context)
        {
            var wrapped = new StatefulEnumerableWrapper<dynamic>(input.GetEnumerator());
            IEnumerator<object> en = func(wrapped).GetEnumerator();
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

        private bool? MoveNext(IEnumerator en, StatefulEnumerableWrapper<object> innerEnumerator, WorkContext context,
                               DocumentStorageActions actions)
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
                Thread.MemoryBarrier(); // force other threads to see this write
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

        #region Nested type: CurrentIndexSearcher

        private class CurrentIndexSearcher
        {
            private bool shouldDisposeWhenThereAreNoUsages;
            private int useCount;
            public IndexSearcher Searcher { get; set; }


            public IDisposable Use()
            {
                Interlocked.Increment(ref useCount);
                return new CleanUp(this);
            }

            public void MarkForDispoal()
            {
                shouldDisposeWhenThereAreNoUsages = true;
            }

            #region Nested type: CleanUp

            private class CleanUp : IDisposable
            {
                private readonly CurrentIndexSearcher parent;

                public CleanUp(CurrentIndexSearcher parent)
                {
                    this.parent = parent;
                }

                #region IDisposable Members

                public void Dispose()
                {
                    var uses = Interlocked.Decrement(ref parent.useCount);
                    if (parent.shouldDisposeWhenThereAreNoUsages && uses == 0)
                        parent.Searcher.Close();
                }

                #endregion
            }

            #endregion
        }

        #endregion
    }
}
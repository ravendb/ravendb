using System;
using System.Collections.Generic;
using System.ComponentModel;
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

namespace Raven.Database.Indexing
{
    public class Index : IDisposable
    {
        private const string documentIdName = "__document_id";
        private ILog log = LogManager.GetLogger(typeof (Index));

        private class CurrentIndexSearcher
        {
            public IndexSearcher Searcher{ get; set;}
            private int useCount;
            private bool shouldDisposeWhenThereAreNoUsages;


            public IDisposable Use()
            {
                Interlocked.Increment(ref useCount);
                return new CleanUp(this);
            }

            private class CleanUp : IDisposable
            {
                private CurrentIndexSearcher parent;

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

        private readonly FSDirectory directory;
        private CurrentIndexSearcher searcher;
        private readonly string name;

        public Index(FSDirectory directory)
        {
            name = directory.GetFile().Name;
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
                if(string.IsNullOrEmpty(query) == false)
                {
                    log.DebugFormat("Issuing query on index {0} for: {1}", name, query);
                    var luceneQuery = new QueryParser("", new StandardAnalyzer()).Parse(query);
                    var search = indexSearcher.Search(luceneQuery);
                    totalSize.Value = search.Length();
                    for (int i = start; i < search.Length() && (i - start) < pageSize; i++)
                    {
                        yield return search.Doc(i).GetField(documentIdName).StringValue();
                    }
                }
                else
                {
                    log.DebugFormat("Browsing index {0}", name);
                    var maxDoc = indexSearcher.MaxDoc();
                    totalSize.Value = maxDoc;
                    for (int i = start; i < maxDoc && (i - start) < pageSize; i++)
                    {
                        yield return indexSearcher.Doc(i).GetField(documentIdName).StringValue();
                    }
                }
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

        public void IndexDocuments(IndexingFunc func, IEnumerable<dynamic> documents)
        {
            int count = 0;
            Write(indexWriter =>
            {
                var currentId = Guid.NewGuid().ToString();
                var luceneDoc = new Document();
                bool shouldRcreateSearcher = false;
                foreach (var doc in func(documents))
                {
                    count++;
                    var fields = new List<Field>();
                    var docId = AddValuesToDocument(fields, doc);
                    if (docId != currentId)
                    {
                        if (luceneDoc.GetFieldsCount() > 0)
                        {
                            indexWriter.UpdateDocument(new Term(documentIdName, docId), luceneDoc);
                            shouldRcreateSearcher = true;
                        }
                        luceneDoc = new Document();
                        currentId = docId;
                        luceneDoc.Add(new Field(documentIdName, docId, Field.Store.YES, Field.Index.UN_TOKENIZED));
                    }
                    foreach (var field in fields)
                    {
                        luceneDoc.Add(field);
                    }
                }
                if (luceneDoc.GetFieldsCount() > 0)
                {
                    indexWriter.UpdateDocument(new Term(documentIdName, currentId), luceneDoc);
                    shouldRcreateSearcher = true;
                }
                return shouldRcreateSearcher;
            });
            log.InfoFormat("Indexed {0} documents for {1}", count, name);
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
            }
        }

        private static string AddValuesToDocument(ICollection<Field> fields, IDictionary<string,object> val)
        {
            var id = val[documentIdName];

            foreach (var  kvp in val)
            {
                if (kvp.Key == documentIdName)
                    continue;
                if (kvp.Value == null)
                    continue;
                fields.Add(new Field(kvp.Key, kvp.Value.ToString(),
                                     Field.Store.YES,
                                     Field.Index.TOKENIZED));
            }
            return (string)id;
        }

        public void Remove(string[] keys)
        {
            Write(writer =>
            {
                if(log.IsDebugEnabled)
                {
                    log.DebugFormat("Deleting ({0}) from {1}", string.Format(", ", keys), name);
                }
                writer.DeleteDocuments(keys.Select(k => new Term(documentIdName, k)).ToArray());
                return true;
            });
        }
    }
}
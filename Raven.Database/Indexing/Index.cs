using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using log4net;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Database.Storage.StorageActions;
using Version = Lucene.Net.Util.Version;

namespace Raven.Database.Indexing
{
    /// <summary>
    /// 	This is a thread safe, single instance for a particular index.
    /// </summary>
    public abstract class Index : IDisposable
    {
        private readonly Directory directory;
        protected readonly ILog log = LogManager.GetLogger(typeof(Index));
        protected readonly string name;
        protected readonly IndexDefinition indexDefinition;
        private CurrentIndexSearcher searcher;
		private readonly object writeLock = new object();
    	private volatile bool disposed;

		[CLSCompliant(false)]
		protected Index(Directory directory, string name, IndexDefinition indexDefinition)
        {
            this.name = name;
            this.indexDefinition = indexDefinition;
            log.DebugFormat("Creating index for {0}", name);
            this.directory = directory;

            // clear any locks that are currently held
            // this may happen if the server crashed while
            // writing to the index
            this.directory.ClearLock("write.lock");

            searcher = new CurrentIndexSearcher
            {
                Searcher = new IndexSearcher(directory, true)
            };
        }

        #region IDisposable Members

        public void Dispose()
        {
			lock (writeLock)
			{
				disposed = true;
				IndexSearcher _;
				using (searcher.Use(out _))
					searcher.MarkForDispoal();
				directory.Close();
			}
        }

        #endregion

        public IEnumerable<IndexQueryResult> Query(IndexQuery indexQuery)
        {
        	IndexSearcher indexSearcher;
        	using (searcher.Use(out indexSearcher))
            {
				var previousDocuments = new HashSet<string>();
            	var luceneQuery = GetLuceneQuery(indexQuery);
            	var start = indexQuery.Start;
            	var pageSize = indexQuery.PageSize;
            	var skippedDocs = 0;
            	var returnedResults = 0;
            	do
            	{
					if(skippedDocs > 0)
					{
						start = start + pageSize;
						pageSize = skippedDocs;
						skippedDocs = 0;
					}
					var search = ExecuteQuery(indexSearcher, luceneQuery, start, pageSize, indexQuery.SortedFields);
					indexQuery.TotalSize.Value = search.totalHits;
					for (var i = start; i < search.totalHits && (i - start) < pageSize; i++)
					{
						var document = indexSearcher.Doc(search.scoreDocs[i].doc);
						if (IsDuplicateDocument(document, indexQuery.FieldsToFetch, previousDocuments))
						{
							skippedDocs++;
							continue;
						}
						returnedResults++;
						yield return RetrieveDocument(document, indexQuery.FieldsToFetch);
					}
				} while (skippedDocs > 0 && returnedResults < indexQuery.PageSize);
            }
        }

    	private static TopDocs ExecuteQuery(IndexSearcher searcher, Query luceneQuery, int start, int pageSize, SortedField[] sortedFields)
        {
        	if(pageSize == int.MaxValue) // we want all docs
        	{
        		var gatherAllCollector = new GatherAllCollector();
        		searcher.Search(luceneQuery, gatherAllCollector);
        		return gatherAllCollector.ToTopDocs();
        	}
            // NOTE: We get Start + Pagesize results back so we have something to page on
			if (sortedFields != null && sortedFields.Length > 0)
            {
                var sort = new Sort(sortedFields.Select(x => x.ToLuceneSortField()).ToArray());
                return searcher.Search(luceneQuery, null, pageSize + start, sort);
            }
        	return searcher.Search(luceneQuery, null, pageSize + start);
        }

        private Query GetLuceneQuery(IndexQuery indexQuery)
        {
            var query = indexQuery.Query;
            Query luceneQuery;
            if (string.IsNullOrEmpty(query))
            {
                log.DebugFormat("Issuing query on index {0} for all documents", name);
                luceneQuery = new MatchAllDocsQuery();
            }
            else
            {
                log.DebugFormat("Issuing query on index {0} for: {1}", name, query);
                luceneQuery = QueryBuilder.BuildQuery(query);
            }
            return luceneQuery;
        }

        private static bool IsDuplicateDocument(Document document, ICollection<string> fieldsToFetch, ISet<string> previousDocuments)
        {
            var docId = document.Get("__document_id");
            if (fieldsToFetch != null && fieldsToFetch.Count > 1 ||
                string.IsNullOrEmpty(docId))
                return false;
            return previousDocuments.Add(docId) == false;
        }

        public abstract void IndexDocuments(AbstractViewGenerator viewGenerator, IEnumerable<object> documents,
                                            WorkContext context,
											IStorageActionsAccessor actions);

		[CLSCompliant(false)]
		protected virtual IndexQueryResult RetrieveDocument(Document document, string[] fieldsToFetch)
		{
			var shouldBuildProjection = fieldsToFetch == null || fieldsToFetch.Length == 0;
			return new IndexQueryResult
			{
				Key = document.Get("__document_id"),
				Projection = shouldBuildProjection ? null : CreateDocumentFromFields(document, fieldsToFetch)
			};
		}

		private static JObject CreateDocumentFromFields(Document document, string[] fieldsToFetch)
    	{
    		return new JObject(
    			fieldsToFetch.Concat(new[] { "__document_id" }).Distinct()
    				.SelectMany(name => document.GetFields(name) ?? new Field[0])
    				.Where(x => x != null)
    				.Select(fld => CreateProperty(fld, document))
    				.GroupBy(x => x.Name)
    				.Select(g =>
    				{
    					if (g.Count() == 1)
    						return g.First();
    					return new JProperty(g.Key,
    					                     g.Select(x => x.Value)
    						);
    				})
    			);
    	}

    	private static JProperty CreateProperty(Field fld, Document document)
    	{
			if (document.GetField(fld.Name() + "_ConvertToJson") != null)
			{
				var val = JsonConvert.DeserializeObject(fld.StringValue());
				return new JProperty(fld.Name(), val);
			}
    		return new JProperty(fld.Name(), fld.StringValue());
    	}

    	protected void Write(Func<IndexWriter, bool> action)
        {
			if(disposed)
				throw new ObjectDisposedException("Index " + name + " has been disposed");
			lock (writeLock)
			{
				bool shouldRecreateSearcher;
				var standardAnalyzer = new StandardAnalyzer(Version.LUCENE_29);
				try
				{
					var indexWriter = new IndexWriter(directory, standardAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED);
					try
					{
						shouldRecreateSearcher = action(indexWriter);
					}
					finally
					{
						indexWriter.Close();
					}
				}
				finally
				{
					standardAnalyzer.Close();

				}
    		if (shouldRecreateSearcher)
                RecreateSearcher();
			}
		}


        protected IEnumerable<object> RobustEnumeration(IEnumerable<object> input, IndexingFunc func,
														IStorageActionsAccessor actions, WorkContext context)
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
							   IStorageActionsAccessor actions)
        {
            try
            {
                actions.Indexing.IncrementIndexingAttempt();
                var moveNext = en.MoveNext();
                if (moveNext == false)
                    actions.Indexing.DecrementIndexingAttempt();
                return moveNext;
            }
            catch (Exception e)
            {
                actions.Indexing.IncrementIndexingFailure();
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
            var dic = current as DynamicJsonObject;
            if (dic == null)
                return null;
            var value = dic.GetValue("__document_id");
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

        public abstract void Remove(string[] keys, WorkContext context);


        private void RecreateSearcher()
        {
        	IndexSearcher _;
        	using (searcher.Use(out _))
            {
                searcher.MarkForDispoal();
                searcher = new CurrentIndexSearcher
                {
                    Searcher = new IndexSearcher(directory, true)
                };
                Thread.MemoryBarrier(); // force other threads to see this write
            }
        }

    	private class CurrentIndexSearcher
        {
            private bool shouldDisposeWhenThereAreNoUsages;
            private int useCount;
            public IndexSearcher Searcher { get; set; }


            public IDisposable Use(out IndexSearcher indexSearcher)
            {
                Interlocked.Increment(ref useCount);
            	indexSearcher = Searcher;
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
    }
}
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
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Storage;
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
		}

		#region IDisposable Members

		public void Dispose()
		{
			directory.Close();
		}

		#endregion

		public IEnumerable<IndexQueryResult> Query(IndexQuery indexQuery)
		{
			var searcher = new IndexSearcher(directory, true);
			try
			{
				var search = ExecuteQuery(searcher, indexQuery, GetLuceneQuery(indexQuery));
				indexQuery.TotalSize.Value = search.totalHits;
				var previousDocuments = new HashSet<string>();
				for (var i = indexQuery.Start; i < search.totalHits && (i - indexQuery.Start) < indexQuery.PageSize; i++)
				{
					var document = searcher.Doc(search.scoreDocs[i].doc);
					if (IsDuplicateDocument(document, indexQuery.FieldsToFetch, previousDocuments))
						continue;
					yield return RetrieveDocument(document, indexQuery.FieldsToFetch);
				}
			}
			finally
			{
				searcher.Close();
			}
		}

		private static TopDocs ExecuteQuery(IndexSearcher searcher, IndexQuery indexQuery, Query luceneQuery)
		{
			TopDocs search;
			if (indexQuery.SortedFields != null && indexQuery.SortedFields.Length > 0)
			{
				var sort = new Sort(indexQuery.SortedFields.Select(x => x.ToLuceneSortField()).ToArray());
				search = searcher.Search(luceneQuery, null, indexQuery.PageSize, sort);
			}
			else
			{
				search = searcher.Search(luceneQuery, null, indexQuery.PageSize);
			}
			return search;
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
			if (fieldsToFetch != null && fieldsToFetch.Count > 1)
				return false;
			return previousDocuments.Add(document.Get("__document_id")) == false;
		}

		public abstract void IndexDocuments(AbstractViewGenerator viewGenerator, IEnumerable<object> documents,
											WorkContext context,
											DocumentStorageActions actions);

		protected abstract IndexQueryResult RetrieveDocument(Document document, string[] fieldsToFetch);

		protected void Write(Func<IndexWriter, bool> action)
		{
			var indexWriter = new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_CURRENT), IndexWriter.MaxFieldLength.UNLIMITED);
			try
			{
				action(indexWriter);
			}
			finally
			{
				indexWriter.Close();
			}
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
	}
}
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Database.Indexing;

namespace Raven.Database.Server.RavenFS.Search
{
	public class IndexStorage : IDisposable
	{
		private const string DateIndexFormat = "yyyy-MM-dd_HH-mm-ss";
		private static readonly string[] NumericIndexFields = new[] { "__size_numeric" };

		private readonly string path;
		private FSDirectory directory;
		private LowerCaseKeywordAnalyzer analyzer;
		private IndexWriter writer;
		private readonly object writerLock = new object();
		private readonly IndexSearcherHolder currentIndexSearcherHolder = new IndexSearcherHolder();

		public IndexStorage(string path, NameValueCollection _)
		{
			this.path = path;
		}

		public void Initialize()
		{
			if (System.IO.Directory.Exists(path) == false)
				System.IO.Directory.CreateDirectory(path);
			directory = FSDirectory.Open(new DirectoryInfo(path));
			if (IndexWriter.IsLocked(directory))
				IndexWriter.Unlock(directory);

			analyzer = new LowerCaseKeywordAnalyzer();
			writer = new IndexWriter(directory, analyzer, IndexWriter.MaxFieldLength.UNLIMITED);
			writer.SetMergeScheduler(new ErrorLoggingConcurrentMergeScheduler());
			currentIndexSearcherHolder.SetIndexSearcher(new IndexSearcher(directory, true));
		}

		public string[] Query(string query, string[] sortFields, int start, int pageSize, out int totalResults)
		{
			IndexSearcher searcher;
			using (GetSearcher(out searcher))
			{
				Query q;
				if (string.IsNullOrEmpty(query))
				{
					q = new MatchAllDocsQuery();
				}
				else
				{
					var queryParser = new RavenQueryParser(analyzer, NumericIndexFields);
					q = queryParser.Parse(query);
				}

				var topDocs = ExecuteQuery(searcher, sortFields, q, pageSize + start);

				var results = new List<string>();

				for (var i = start; i < pageSize + start && i < topDocs.TotalHits; i++)
				{
					var document = searcher.Doc(topDocs.ScoreDocs[i].Doc);
					results.Add(document.Get("__key"));
				}
				totalResults = topDocs.TotalHits;
				return results.ToArray();
			}
		}

		private TopDocs ExecuteQuery(IndexSearcher searcher, string[] sortFields, Query q, int size)
		{
			TopDocs topDocs;
			if (sortFields != null && sortFields.Length > 0)
			{
				var sort = new Sort(sortFields.Select(field =>
				{
					var desc = field.StartsWith("-");
					if (desc)
						field = field.Substring(1);
					return new SortField(field, SortField.STRING, desc);
				}).ToArray());
				topDocs = searcher.Search(q, null, size, sort);
			}
			else
			{
				topDocs = searcher.Search(q, null, size);
			}
			return topDocs;
		}

		public virtual void Index(string key, NameValueCollection metadata)
		{
			lock (writerLock)
			{
				var lowerKey = key.ToLowerInvariant();
				var doc = CreateDocument(lowerKey, metadata);

				foreach (var metadataKey in metadata.AllKeys)
				{
					var values = metadata.GetValues(metadataKey);
					if (values == null)
						continue;

					foreach (var value in values)
					{
						doc.Add(new Field(metadataKey, value, Field.Store.NO, Field.Index.ANALYZED_NO_NORMS));
					}
				}

				writer.DeleteDocuments(new Term("__key", lowerKey));
				writer.AddDocument(doc);
				// yes, this is slow, but we aren't expecting high writes count
				writer.Commit();
				ReplaceSearcher();
			}
		}

		private static Document CreateDocument(string lowerKey, NameValueCollection metadata)
		{
			var doc = new Document();
			doc.Add(new Field("__key", lowerKey, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));

			var fileName = Path.GetFileName(lowerKey);
			Debug.Assert(fileName != null);
			doc.Add(new Field("__fileName", fileName, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
			// the reversed version of the file name is used to allow searches that start with wildcards
			char[] revFileName = fileName.ToCharArray();
			Array.Reverse(revFileName);
			doc.Add(new Field("__rfileName", new string(revFileName), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));

			int level = 0;
			var directoryName = Path.GetDirectoryName(lowerKey);
			do
			{
				level += 1;
				directoryName = (string.IsNullOrEmpty(directoryName) ? "" : directoryName.Replace("\\", "/"));
				if (directoryName.StartsWith("/") == false)
					directoryName = "/" + directoryName;
				doc.Add(new Field("__directory", directoryName, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
				directoryName = Path.GetDirectoryName(directoryName);
			} while (directoryName != null);
			doc.Add(new Field("__modified", DateTime.UtcNow.ToString(DateIndexFormat, CultureInfo.InvariantCulture), Field.Store.NO,
							  Field.Index.NOT_ANALYZED_NO_NORMS));
			doc.Add(new Field("__level", level.ToString(CultureInfo.InvariantCulture), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
			long len;
			if (long.TryParse(metadata["Content-Length"], out len))
			{
				doc.Add(new Field("__size", len.ToString("D20"), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
				doc.Add(new NumericField("__size_numeric", Field.Store.NO, true).SetLongValue(len));
			}

			return doc;
		}

		internal IDisposable GetSearcher(out IndexSearcher searcher)
		{
			return currentIndexSearcherHolder.GetSearcher(out searcher);
		}

		public void Dispose()
		{
			analyzer.Close();
			if (currentIndexSearcherHolder != null)
			{
				currentIndexSearcherHolder.SetIndexSearcher(null);
			}
			writer.Close();
			directory.Close();
		}

		public void Delete(string key)
		{
			var lowerKey = key.ToLowerInvariant();

			lock (writerLock)
			{
				writer.DeleteDocuments(new Term("__key", lowerKey));
				writer.Optimize();
				writer.Commit();
				ReplaceSearcher();
			}
		}

		private void ReplaceSearcher()
		{
			currentIndexSearcherHolder.SetIndexSearcher(new IndexSearcher(writer.GetReader()));
		}

		public IEnumerable<string> GetTermsFor(string field, string fromValue)
		{
			IndexSearcher searcher;
			using (GetSearcher(out searcher))
			{
				var termEnum = searcher.IndexReader.Terms(new Term(field, fromValue ?? string.Empty));
				try
				{
					if (string.IsNullOrEmpty(fromValue) == false) // need to skip this value
					{
						while (termEnum.Term == null || fromValue.Equals(termEnum.Term.Text))
						{
							if (termEnum.Next() == false)
								yield break;
						}
					}
					while (termEnum.Term == null ||
						field.Equals(termEnum.Term.Field))
					{
						if (termEnum.Term != null)
						{
							var item = termEnum.Term.Text;
							yield return item;
						}

						if (termEnum.Next() == false)
							break;
					}
				}
				finally
				{
					termEnum.Close();
				}
			}
		}
	}
}
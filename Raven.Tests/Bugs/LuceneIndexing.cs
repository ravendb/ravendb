using System;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Database.Indexing;
using Xunit;
using Version = Lucene.Net.Util.Version;

namespace Raven.Tests.Bugs
{
	public class LuceneIndexing
	{
		[Fact]
		public void MrsJones()
		{
			var dir = new RAMDirectory();
			var analyzer = new LowerCaseKeywordAnalyzer();
			var writer = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED);
			var document = new Lucene.Net.Documents.Document();
			document.Add(new Field("Name", "MRS. SHABA", Field.Store.NO, Field.Index.ANALYZED_NO_NORMS));
			writer.AddDocument(document);

			writer.Close(true);
			

			var searcher = new IndexSearcher(dir, true);

			var termEnum = searcher.GetIndexReader().Terms();
			while (termEnum.Next())
			{
				var buffer = termEnum.Term().Text();
				Console.WriteLine(buffer);
			}

			var queryParser = new RangeQueryParser(Version.LUCENE_29, "", analyzer);
			var query = queryParser.Parse("Name:\"MRS. S*\"");
			Console.WriteLine(query);
			var result = searcher.Search(query, 10);

			Assert.NotEqual(0,result.TotalHits);
		}
	}
}
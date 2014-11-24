using System;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Database.Indexing;
using Raven.Tests.Common;

using Xunit;
using Version = Lucene.Net.Util.Version;

namespace Raven.Tests.Bugs
{
	public class LuceneIndexing : NoDisposalNeeded
	{
		[Fact]
		public void MrsJones()
		{
			using (var dir = new RAMDirectory())
		    using (var analyzer = new LowerCaseKeywordAnalyzer())
		    {
		        using (var writer = new IndexWriter(dir, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED))
		        {
		            var document = new Lucene.Net.Documents.Document();
		            document.Add(new Field("Name", "MRS. SHABA", Field.Store.NO, Field.Index.ANALYZED_NO_NORMS));
		            writer.AddDocument(document);
		        }

		        var searcher = new IndexSearcher(dir, true);

		        var termEnum = searcher.IndexReader.Terms();
		        while (termEnum.Next())
		        {
		            var buffer = termEnum.Term.Text;
		            Console.WriteLine(buffer);
		        }

		        var queryParser = new RangeQueryParser(Version.LUCENE_29, "", analyzer);
		        var query = queryParser.Parse("Name:\"MRS. S*\"");
		        Console.WriteLine(query);
		        var result = searcher.Search(query, 10);

		        Assert.NotEqual(0, result.TotalHits);
		    }
		}
	}
}
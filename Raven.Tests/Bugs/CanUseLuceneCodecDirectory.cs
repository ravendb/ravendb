using System;
using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class CanUseLuceneCodecDirectory : IDisposable
	{
		private const string Path = "TestLuceneCodecDir";

		public CanUseLuceneCodecDirectory()
		{
			IOExtensions.DeleteDirectory(Path);
		}

		public void Dispose()
		{
			IOExtensions.DeleteDirectory(Path);
		}

		[Fact]
		public void WithoutGettingErrors()
		{
			using(var luceneCodecDirectory = new LuceneCodecDirectory(Path, Enumerable.Empty<AbstractIndexCodec>()))
			using(var simpleAnalyzer = new SimpleAnalyzer())
			{
				using (var w = new IndexWriter(luceneCodecDirectory, simpleAnalyzer, true, IndexWriter.MaxFieldLength.UNLIMITED))
				{
					var doc = new Lucene.Net.Documents.Document();
					doc.Add(new Field("test", "value", Field.Store.YES, Field.Index.ANALYZED));
					w.AddDocument(doc);
				}

				using(var s = new IndexSearcher(luceneCodecDirectory))
				{
					var termQuery = new TermQuery(new Term("test", "value"));
					var topDocs = s.Search(termQuery, 10);
					Assert.Equal(1, topDocs.TotalHits);
				}
			}

		}
	}
}
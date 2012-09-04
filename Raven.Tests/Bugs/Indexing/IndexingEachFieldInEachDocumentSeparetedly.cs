using System;
using System.ComponentModel.Composition.Hosting;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Raven.Client.Document;
using Raven.Database.Plugins;
using Xunit;
using Version = Lucene.Net.Util.Version;
using System.Linq;

namespace Raven.Tests.Bugs.Indexing
{
	public class IndexingEachFieldInEachDocumentSeparetedly : RavenTest
	{
		[Fact]
		public void ForIndexing()
		{
			using (var store = NewDocumentStore())
			{
				store.Configuration.Catalog.Catalogs.Add(new TypeCatalog(typeof(MyAnalyzerGenerator)));
				using (var s = store.OpenSession())
				{
					s.Store(new {Name = "Ayende Rahien"});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var objects = s.Advanced.LuceneQuery<object>()
						.WhereEquals("Name", "Ayende")
						.ToArray();

					Assert.NotEmpty(objects);
				}
			}
		}

		public class MyAnalyzerGenerator : AbstractAnalyzerGenerator
		{
			public override Analyzer GenerateAnalyzerForIndexing(string indexName, Lucene.Net.Documents.Document document, Analyzer previousAnalyzer)
			{
				return new StandardAnalyzer(Version.LUCENE_29);
			}

			public override Analyzer GenerateAnalzyerForQuerying(string indexName, string query, Analyzer previousAnalyzer)
			{
				return new StandardAnalyzer(Version.LUCENE_29);
			}
		}
	}
}

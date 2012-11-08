using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Spatial.Prefix.Tree;
using Lucene.Net.Spatial.Queries;
using Lucene.Net.Store;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Raven.Json.Linq;
using Spatial4n.Core.Context.Nts;
using Xunit;

namespace Raven.Tests.Spatial
{
	public class SimonBartlett : RavenTest
	{
		[Fact]
		public void PureLucene()
		{
			using (var dir = new RAMDirectory())
			{
				using (var keywordAnalyzer = new KeywordAnalyzer())
				using (var writer = new IndexWriter(dir, keywordAnalyzer, true, IndexWriter.MaxFieldLength.UNLIMITED))
				{
					var doc = new Lucene.Net.Documents.Document();

					var writeShape = NtsSpatialContext.GEO.ReadShape("LINESTRING (0 0, 1 1, 2 1)");
					var writeStrategy = SpatialIndex.CreateStrategy("WKT", SpatialSearchStrategy.GeohashPrefixTree, GeohashPrefixTree.GetMaxLevelsPossible());
					foreach (var f in writeStrategy.CreateIndexableFields(writeShape))
					{
						doc.Add(f);
					}
					writer.AddDocument(doc);
					writer.Commit();
				}


				var shape = NtsSpatialContext.GEO.ReadShape("LINESTRING (1 0, 1 1, 1 2)");
				SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, shape);
				var strategy = SpatialIndex.CreateStrategy("WKT", SpatialSearchStrategy.GeohashPrefixTree, GeohashPrefixTree.GetMaxLevelsPossible());
				var makeQuery = strategy.MakeQuery(args);
				using(var search = new IndexSearcher(dir))
				{
					var topDocs = search.Search(makeQuery, 5);
					Assert.Equal(1, topDocs.TotalHits);
				}
			}
		}

		[Fact]
		public void LineStringsShouldIntersect()
		{
			using (var store = new EmbeddableDocumentStore { RunInMemory = true })
			{
				store.Initialize();
				store.ExecuteIndex(new GeoIndex());

				using (var session = store.OpenSession())
				{
					session.Store(new GeoDocument { WKT = "LINESTRING (0 0, 1 1, 2 1)" });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var matches = session.Query<RavenJObject, GeoIndex>()
						.Customize(x =>
						{
							x.RelatesToShape("WKT", "LINESTRING (1 0, 1 1, 1 2)", SpatialRelation.Intersects);
							x.WaitForNonStaleResults();
						}).Any();

					Assert.True(matches);
				}
			}
		}

		public class GeoDocument
		{
			public string WKT { get; set; }
		}

		public class GeoIndex : AbstractIndexCreationTask<GeoDocument>
		{
			public GeoIndex()
			{
				Map = docs => from doc in docs
							  select new { _ = SpatialGenerate("WKT", doc.WKT) };
			}
		}
	}
}

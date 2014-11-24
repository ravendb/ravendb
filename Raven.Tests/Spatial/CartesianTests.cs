using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Spatial
{
	public class CartesianTests : RavenTest
	{
		public class SpatialDoc
		{
			public string Id { get; set; }
			public object Name { get; set; }
			public string WKT { get; set; }
		}

		public class CartesianIndex : AbstractIndexCreationTask<SpatialDoc>
		{
			public CartesianIndex()
			{
				Map = docs => from doc in docs select new { doc.Name, doc.WKT };

				Index(x => x.Name, FieldIndexing.Analyzed);
				Store(x => x.Name, FieldStorage.Yes);

				Spatial(x => x.WKT, x => x.Cartesian.QuadPrefixTreeIndex(12, new SpatialBounds(0, 0, 2000, 2000)));
			}
		}

		[Fact]
		public void Points()
		{
			using (var store = NewDocumentStore())
			{
				store.Initialize();
				store.ExecuteIndex(new CartesianIndex());

				using (var session = store.OpenSession())
				{
					session.Store(new SpatialDoc { WKT = "POINT (1950 1950)", Name = new { sdsdsd = "sdsds", sdsdsds="sdsds"} });
					session.Store(new SpatialDoc { WKT = "POINT (50 1950)", Name = "dog" });
					session.Store(new SpatialDoc { WKT = "POINT (1950 50)", Name = "cat" });
					session.Store(new SpatialDoc { WKT = "POINT (50 50)", Name = "dog" });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var matches = session.Query<RavenJObject, CartesianIndex>()
						.Customize(x => x.WithinRadiusOf("WKT", 70, 1900, 1900))
						.Any();

					Assert.True(matches);
				}
			}
		}
	}
}

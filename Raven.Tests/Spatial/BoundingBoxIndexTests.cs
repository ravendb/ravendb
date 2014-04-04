using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Spatial
{
	public class BoundingBoxIndexTests : RavenTestBase
	{
		[Fact]
		public void BoundingBoxTest()
		{
			// X XXX X
			// X XXX X
			// X XXX X
			// X	 X
			// XXXXXXX

			var polygon = "POLYGON ((0 0, 0 5, 1 5, 1 1, 5 1, 5 5, 6 5, 6 0, 0 0))";
			var rectangle1 = "2 2 4 4";
			var rectangle2 = "6 6 10 10";
			var rectangle3 = "0 0 6 6";

			using (var store = NewDocumentStore())
			{
				store.Initialize();
				new BBoxIndex().Execute(store);
				new QuadTreeIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new SpatialDoc{Shape = polygon});
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var result = session.Query<SpatialDoc>()
										.Count();

					Assert.Equal(1, result);
				}

				using (var session = store.OpenSession())
				{
					var result = session.Query<SpatialDoc, BBoxIndex>()
										.Spatial(x => x.Shape, x => x.Intersects(rectangle1))
										.Count();

					Assert.Equal(1, result);
				}

				using (var session = store.OpenSession())
				{
					var result = session.Query<SpatialDoc, BBoxIndex>()
										.Spatial(x => x.Shape, x => x.Intersects(rectangle2))
										.Count();

					Assert.Equal(0, result);
				}

				using (var session = store.OpenSession())
				{
					var result = session.Query<SpatialDoc, BBoxIndex>()
										.Spatial(x => x.Shape, x => x.Disjoint(rectangle2))
										.Count();

					Assert.Equal(1, result);
				}

				using (var session = store.OpenSession())
				{
					var result = session.Query<SpatialDoc, BBoxIndex>()
										.Spatial(x => x.Shape, x => x.Within(rectangle3))
										.Count();

					Assert.Equal(1, result);
				}

				using (var session = store.OpenSession())
				{
					var result = session.Query<SpatialDoc, QuadTreeIndex>()
										.Spatial(x => x.Shape, x => x.Intersects(rectangle2))
										.Count();

					Assert.Equal(0, result);
				}

				using (var session = store.OpenSession())
				{
					var result = session.Query<SpatialDoc, QuadTreeIndex>()
										.Spatial(x => x.Shape, x => x.Intersects(rectangle1))
										.Count();

					Assert.Equal(0, result);
				}
			}
		}

		public class SpatialDoc
		{
			 public string Id { get; set; }
			 public string Shape { get; set; }
		}

		public class BBoxIndex : AbstractIndexCreationTask<SpatialDoc> 
		{
			public BBoxIndex()
			{
				Map = docs => from doc in docs
							  select new
									 {
										 doc.Shape
									 };

				Spatial(x => x.Shape, x => x.Cartesian.BoundingBoxIndex());
			}
		}

		public class QuadTreeIndex : AbstractIndexCreationTask<SpatialDoc> 
		{
			public QuadTreeIndex()
			{
				Map = docs => from doc in docs
							  select new
									 {
										 doc.Shape
									 };

				Spatial(x => x.Shape, x => x.Cartesian.QuadPrefixTreeIndex(6, new SpatialBounds(0, 0, 16, 16)));
			}
		}
	}
}

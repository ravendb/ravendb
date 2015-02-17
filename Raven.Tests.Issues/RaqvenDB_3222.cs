using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RaqvenDB_3222 : RavenTest
	{
		public class MySpatialEntity
		{
			public string Name { get; set; }
			public string WKT { get; set; }
		}

		public class SpatialIndexForTest : AbstractIndexCreationTask<MySpatialEntity>
		{
			public SpatialIndexForTest()
			{
				Map = docs => from doc in docs
							  select new MySpatialEntity()
							  {
								  Name = doc.Name,
								  WKT = doc.WKT
							  };
				Store(x => x.Name, FieldStorage.Yes);
				Spatial(x => x.WKT, x => x.Cartesian.QuadPrefixTreeIndex(12, new SpatialBounds(0, 0, 200, 200)));
			}
		}

		[Fact]
		public void TDSQ()
		{
			using (var store = NewRemoteDocumentStore(fiddler: true))
			{
				new SpatialIndexForTest().Execute(store);

				using (var bulkInsert = store.BulkInsert())
				{
					for (int i = 0; i < 100; i++)
					{
						bulkInsert.Store(new MySpatialEntity()
						{
							Name = (i % 4).ToString(),
							WKT = string.Format("Point ({0} {1})", 99 + i % 10 + 1, 99 + i % 10 + 1)
						});
					}
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var result = session.Query<MySpatialEntity, SpatialIndexForTest>()
						.Customize(x => x.WithinRadiusOf("WKT", 98, 99, 99))
						.Select(x => x.Name)
						.Distinct()
						.ToArray();
					Assert.Equal(result.Length, 4);
				}
			}
		}
	}
}

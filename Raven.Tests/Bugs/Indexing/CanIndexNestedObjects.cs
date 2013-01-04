using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.Bugs.Indexing
{
	public class CanIndexNestedObjects : RavenTest
	{
		public class NestedObject
		{
			public string Name { get; set; }
			public int Quantity { get; set; }
		}

		public class ContainerObject
		{
			public string ContainerName { get; set; }
			public IEnumerable<NestedObject> Items { get; set; }
		}

		public class IndexEntry
		{
			public string ContainerName { get; set; }
			public string Name { get; set; }
			public int Quantity { get; set; }
		}

		public class NestedObjectIndex : AbstractIndexCreationTask<ContainerObject, IndexEntry>
		{
			public NestedObjectIndex()
			{
				Map = docs => from doc in docs
							  from item in doc.Items
							  select new
							  {
								  doc.ContainerName,
								  item.Name,
								  item.Quantity
							  };
				Store(x => x.Name, FieldStorage.Yes);
				Store(x => x.ContainerName, FieldStorage.Yes);
				Store(x => x.Quantity, FieldStorage.Yes);
			}
		}

		[Fact]
		public void SimpleInsertAndRead()
		{
			string expectedContainerName = "someContainer123098";
			string expectedItemName = "someItem456";
			int expectedQuantity = 123;

			using (var store = NewDocumentStore())
			{
				new NestedObjectIndex().Execute(store);


				using (var s = store.OpenSession())
				{
					s.Store(new ContainerObject()
					{
						ContainerName = expectedContainerName,
						Items = new[]
						{
							new NestedObject()
							{
								Name = expectedItemName,
								Quantity = expectedQuantity
							},
							new NestedObject()
							{
								Name = "something Else",
								Quantity = 345
							}
		                }
					});

					s.SaveChanges();
				}

				//  the index has two objects
				using (var s = store.OpenSession())
				{
					var result = s.Query<ContainerObject, NestedObjectIndex>()
						.Customize(q => q.WaitForNonStaleResultsAsOfNow())
						.Count();

					Assert.Equal(2, result);
				}

				//  and the index can be queried
				using (var s = store.OpenSession())
				{
					var result = s.Query<ContainerObject, NestedObjectIndex>()
						.Customize(q => q.WaitForNonStaleResultsAsOfNow())
						.AsProjection<IndexEntry>()
						.Where(o => o.Name == expectedItemName)
						.Single();

					Assert.Equal(expectedContainerName, result.ContainerName);
					Assert.Equal(expectedItemName, result.Name);
					Assert.Equal(expectedQuantity, result.Quantity);
				}
			}
		}
	}
}

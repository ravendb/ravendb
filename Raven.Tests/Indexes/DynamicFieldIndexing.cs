using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Indexes
{
	public class DynamicFieldIndexing : RavenTest
	{
		public class Item
		{
			public string Id { get; set; }
			public Dictionary<string, string> Values { get; set; }
		}

		public class WithDynamicIndex : AbstractIndexCreationTask<Item>
		{
			public WithDynamicIndex()
			{
				Map = items =>
				      from item in items
				      select new
				             {
				             	_ = item.Values.Select(x => CreateField(x.Key, x.Value))
				             };
			}
		}

		[Fact]
		public void CanSearchDynamically()
		{
			using (var store = NewDocumentStore())
			{
				new WithDynamicIndex().Execute(store);

				using (var s = store.OpenSession())
				{
					s.Store(new Item
					        {
					        	Values = new Dictionary<string, string>
					        	         {
					        	         	{"Name", "Fitzchak"},
					        	         	{"User", "Admin"}
					        	         }
					        });

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
                    var items = s.Advanced.DocumentQuery<Item, WithDynamicIndex>()
						.WaitForNonStaleResults()
						.WhereEquals("Name", "Fitzchak")
						.ToList();

					Assert.NotEmpty(items);
				}
			}
		}

		[Fact]
		public void CanSearchDynamicFieldWithSpaces()
		{
			using (var store = NewDocumentStore())
			{
				new WithDynamicIndex().Execute(store);

				using (var s = store.OpenSession())
				{
					s.Store(new Item
					{
						Values = new Dictionary<string, string>
					        	         {
					        	         	{"First Name", "Fitzchak"},
					        	         	{"User", "Admin"}
					        	         }
					});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
                    var items = s.Advanced.DocumentQuery<Item, WithDynamicIndex>()
						.WaitForNonStaleResults()
						.WhereEquals("First Name", "Fitzchak")
						.ToList();

					Assert.NotEmpty(items);
				}
			}
		}
	}
}

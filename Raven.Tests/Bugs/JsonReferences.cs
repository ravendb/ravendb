using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class JsonReferences
	{
		[Fact]
		public void can_index_on_a_reference2()
		{
			using (var store = new EmbeddableDocumentStore
			{
				RunInMemory = true
			})
			{

				store.Initialize();
				using (var session = store.OpenSession())
				{
					var category = new Category()
					{
						Name = "Parent"
					};

					category.Add(new Category()
					{
						Name = "Child"
					});

					session.Store(category);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var results0 = session.Query<Category>()
						.Customize(x=>x.WaitForNonStaleResults(TimeSpan.FromHours(1)))
						.ToList();
					Assert.Equal(1, results0.Count);

					// WORKS
					var results1 = session.Query<Category>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Children.Any(y => y.Name == "Child")).
						ToList();
					Assert.Equal(1, results1.Count);

					// FAILS
					var results2 = session.Query<Category>()
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Children.Any(y => y.Parent.Name == "Parent"))
						.ToList();
					Assert.Equal(1, results2.Count);
				}
			}
		}

		[JsonObject(IsReference = true)]
		public class Category
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public Category Parent { get; set; }
			public List<Category> Children { get; set; }

			public Category()
			{
				Children = new List<Category>();
			}

			public void Add(Category category)
			{
				category.Parent = this;
				Children.Add(category);
			}
		}
	}

}

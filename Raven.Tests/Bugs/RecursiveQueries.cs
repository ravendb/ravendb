using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.Bugs
{
	public class RecursiveQueries : RavenTest
	{
		[Fact]
		public void ShouldBePossible()
		{
			using(var store = NewDocumentStore())
			{
				new CategoryWithParentsAndChildren().Execute(store);

				using(var session = store.OpenSession())
				{
					var root = new Category
					{
						Name = "Root"
					};
					session.Store(root);
					var category = new Category
					{
						Name = "Child",
						ParentId = root.Id
					};
					session.Store(category);
					session.Store(new Category
					{
						Name = "Grandchild",
						ParentId = category.Id
					});
					session.SaveChanges();
				}
				
				using(var session = store.OpenSession())
				{
					List<CategoryHeaderWithParents> categoryHeaderWithParentses = session.Query<CategoryHeaderWithParents, CategoryWithParentsAndChildren>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Where(x=>x.Name == "Grandchild")
						.ToList();

					Assert.Equal("Grandchild", categoryHeaderWithParentses[0].Name);
					Assert.Equal("Grandchild", categoryHeaderWithParentses[0].Parents[0].Name);
					Assert.Equal("Child", categoryHeaderWithParentses[0].Parents[1].Name);
					Assert.Equal("Root", categoryHeaderWithParentses[0].Parents[2].Name);
				}
			}
		}

		public class CategoryWithParentsAndChildren : AbstractIndexCreationTask<Category>
		{
			public CategoryWithParentsAndChildren()
			{
				Map = categories => from category in categories
				                    select new {category.Id, category.Name, category.ParentId};
				TransformResults = (database, categories) =>
								   from category in categories
								   let parentCategories = Recurse(category, c => database.Load<Category>(c.ParentId))
								   select new
								   {
									   category.Id,
									   category.Name,
									   Parents =
									   (
										   from parent in parentCategories
										   select new { parent.Id, parent.Name }
									   )
								   };
			}
		}

		public class CategoryHeader
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class CategoryHeaderWithParents : CategoryHeader
		{
			public CategoryHeader[] Parents { get; set; }
		}

		public class Category
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string ParentId { get; set; }
		}
	}
}
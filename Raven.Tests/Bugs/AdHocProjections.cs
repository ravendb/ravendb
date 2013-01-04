using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class AdHocProjections : RavenTest
	{
		[Fact]
		public void Query_can_project_to_a_different_model()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Entity
					              	{
					              		Id = 1,
					              		Category = new Category { Title = "Category Title" }
					              	});

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var viewModel = (from entity in session.Query<Entity>()
										.Customize(x=>x.WaitForNonStaleResults())
					                 select new EntityViewModel
					                        	{
					                        		Id = entity.Id,
					                        		CategoryTitle = entity.Category.Title
					                        	}).SingleOrDefault();

					Assert.NotNull(viewModel);
					Assert.Equal(1, viewModel.Id);
					Assert.Equal("Category Title", viewModel.CategoryTitle);
				}
			}
		}

		#region Nested type: Category

		public class Category
		{
			public string Title { get; set; }
		}

		#endregion

		#region Nested type: Entity

		public class Entity
		{
			public int Id { get; set; }
			public Category Category { get; set; }
		}

		#endregion

		#region Nested type: EntityViewModel

		public class EntityViewModel
		{
			public int Id { get; set; }
			public string CategoryTitle { get; set; }
		}

		#endregion
	}
}
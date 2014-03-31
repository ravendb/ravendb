using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class bikkies : RavenTest
	{
		[Fact]
		public void ShouldGetCategoryValues()
		{
			using(var store= NewDocumentStore())
			{
				new Activity_WithCategory().Execute(store);
				BuildData(store);

				WaitForIndexing(store);

				using(var s = store.OpenSession())
				{
					var activityVms = s.Query<ActivityVM, Activity_WithCategory>().ToArray();
					foreach (var activityVm in activityVms)
					{
						Assert.NotNull(activityVm.CatId);
						Assert.NotNull(activityVm.CategoryColor);
						Assert.NotNull(activityVm.CategoryName);
					}
				}
			}
		}

		public class Activity
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public CategoryRef Category { get; set; }

		}
		public class ActivityVM
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string CategoryName { get; set; }
			public string CategoryColor { get; set; }
			public string CatId { get; set; }

		}
		public class CategoryRef
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
		public class Category
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Color { get; set; }
		}

		private static void BuildData(IDocumentStore ds)
		{
			var c = new Category() { Color = "Green", Name = "Work-Based Learning" };
			var c2 = new Category() { Color = "Red", Name = "Research" };


			using (var session = ds.OpenSession())
			{
				session.Store(c);
				session.Store(c2);
				session.SaveChanges();
			}
			var a = new Activity() { Name = "Learning by Doing", Category = new CategoryRef() { Id = c.Id, Name = c.Name } };
			var a2 = new Activity() { Name = "Private Study", Category = new CategoryRef() { Id = c2.Id, Name = c2.Name } };
			using (var session = ds.OpenSession())
			{
				session.Store(a);
				session.Store(a2);
				session.SaveChanges();
			}


		}
		public class Activity_WithCategory : AbstractIndexCreationTask<Activity>
		{
			public Activity_WithCategory()
			{
				Map = activities => from a in activities
									select new { CatID = a.Category.Id };

				TransformResults = (db, activities) => from a2 in activities
													   let c = db.Load<Category>(a2.Category.Id)
													   select new
													   {
														   a2.Id,
														   a2.Name,
														   CatId = a2.Category.Id,
														   CategoryName = c.Name,
														   CategoryColor = c.Color
													   };


			}
		}
	}
}
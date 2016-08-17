using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class bikkies : RavenTestBase
    {
        [Fact]
        public async Task ShouldGetCategoryValues()
        {
            using (var store = await GetDocumentStore())
            {
                new Activity_WithCategory().Execute(store);
                new Activity_WithCategoryTransformer().Execute(store);
                BuildData(store);

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var activityVms = s.Query<ActivityVM, Activity_WithCategory>().TransformWith<Activity_WithCategoryTransformer, ActivityVM>().ToArray();
                    foreach (var activityVm in activityVms)
                    {
                        Assert.NotNull(activityVm.CatId);
                        Assert.NotNull(activityVm.CategoryColor);
                        Assert.NotNull(activityVm.CategoryName);
                    }
                }
            }
        }

        private class Activity
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public CategoryRef Category { get; set; }

        }

        private class ActivityVM
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string CategoryName { get; set; }
            public string CategoryColor { get; set; }
            public string CatId { get; set; }

        }

        private class CategoryRef
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Category
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
        private class Activity_WithCategory : AbstractIndexCreationTask<Activity>
        {
            public Activity_WithCategory()
            {
                Map = activities => from a in activities
                                    select new { CatID = a.Category.Id };
            }
        }

        private class Activity_WithCategoryTransformer : AbstractTransformerCreationTask<Activity>
        {
            public Activity_WithCategoryTransformer()
            {
                TransformResults = activities => from a2 in activities
                                                 let c = LoadDocument<Category>(a2.Category.Id)
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

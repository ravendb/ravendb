using FastTests;
using Xunit;
using System.Linq;

namespace SlowTests.Bugs
{
    public class AdHocProjections : RavenTestBase
    {
        [Fact]
        public void Query_can_project_to_a_different_model()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Entity
                                    {
                                        Id = "1",
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
                    Assert.Equal("1", viewModel.Id);
                    Assert.Equal("Category Title", viewModel.CategoryTitle);
                }
            }
        }

        #region Nested type: Category

        private class Category
        {
            public string Title { get; set; }
        }

        #endregion

        #region Nested type: Entity

        private class Entity
        {
            public string Id { get; set; }
            public Category Category { get; set; }
        }

        #endregion

        #region Nested type: EntityViewModel

        private class EntityViewModel
        {
            public string Id { get; set; }
            public string CategoryTitle { get; set; }
        }

        #endregion
    }
}

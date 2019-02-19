using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class BuildStarted : RavenTestBase
    {
        private class TestModel
        {
            public string Id { get; set; }
        }

        //This test attempts to lookup an existing item when none exist in the
        //database. It returns null which that variable is assigned a new value
        //storing that new value, however, fails.
        [Fact]
        public void DocumentStoreFailsWhenGrabbingNonExistingItemAndStoringNewOne()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    TestModel testModelItem = session.Query<TestModel>().SingleOrDefault(t => t.Id == 1.ToString()) ??
                                              new TestModel { Id = 1.ToString() };
                    Assert.NotNull(testModelItem);
                    session.Store(testModelItem);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var list = session.Query<TestModel>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(1, list.Count());
                }
            }
        }


        //This test adds a new item to the database and then performs a lookup on a
        //non existing item. Sets the variable to a new value and stores it.
        //works like expected
        [Fact]
        public void DocumentStoreWorksWhenAddingItemAndThenGrabbingNonExistingItemAndStoringNewOne()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestModel { Id = 1.ToString() });
                    session.SaveChanges();

                    TestModel testModelItem = session.Query<TestModel>().SingleOrDefault(t => t.Id == 2.ToString()) ??
                                                  new TestModel { Id = 2.ToString() };
                    Assert.NotNull(testModelItem);
                    session.Store(testModelItem);
                    session.SaveChanges();

                    var list = session.Query<TestModel>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(2, list.Count());
                }
            }
        }

        //This test adds a new item to the database and then deletes it and then
        //performs a lookup on a non existing item. Sets the variable to a new 
        //value and stores it, however, fails.
        [Fact]
        public void DocumentStoreWorksWhenAddingItemThenDeletingItAndThenGrabbingNonExistingItemAndStoringNewOne()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var deletedModel = new TestModel { Id = 1.ToString() };
                    session.Store(deletedModel);
                    session.SaveChanges();

                    session.Delete(deletedModel);
                    session.SaveChanges();

                    TestModel testModelItem = session.Query<TestModel>().SingleOrDefault(t => t.Id == 2.ToString()) ??
                                                  new TestModel { Id = 2.ToString() };
                    Assert.NotNull(testModelItem);
                    session.Store(testModelItem);
                    session.SaveChanges();

                    var list = session.Query<TestModel>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(1, list.Count());
                }
            }
        }
    }
}

using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11111 : RavenTestBase
    {
        [Fact]
        public void QueryNullableTest()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.OnBeforeQuery += (_, e) => e.QueryCustomization.WaitForNonStaleResults()
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { Id = "Test true1", IsLocked = true });
                    session.Store(new TestDoc { Id = "Test true2", IsLocked = true });
                    session.Store(new TestDoc { Id = "Test null1", IsLocked = null });
                    session.Store(new TestDoc { Id = "Test false1", IsLocked = false });
                    session.Store(new TestDoc { Id = "Test true3", IsLocked = true });
                    session.Store(new TestDoc { Id = "Test null2", IsLocked = null });
                    session.Store(new TestDoc { Id = "Test true4", IsLocked = true });
                    session.Store(new TestDoc { Id = "Test false2", IsLocked = false });
                    session.Store(new TestDoc { Id = "Test true5", IsLocked = true });
                    session.Store(new TestDoc { Id = "Test null3", IsLocked = null });
                    session.Store(new TestDoc { Id = "Test true6", IsLocked = true });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var query1 = session.Query<TestDoc>().Where(x => x.IsLocked == null);
                    var uery1Result = query1.ToList();
                    Assert.Equal(3, uery1Result.Count);

                    var query2 = session.Query<TestDoc>().Where(x => x.IsLocked == true);
                    var uery2Result = query2.ToList();
                    Assert.Equal(6, uery2Result.Count);

                    var query3 = session.Query<TestDoc>().Where(x => x.IsLocked == false);
                    var uery3Result = query3.ToList();
                    Assert.Equal(2, uery3Result.Count);
                    // This fails, there are only results IsLocked == null
                }
            }
        }

        public class TestDoc

        {
            public string Id { get; set; }

            public bool? IsLocked { get; set; }
        }
    }
}

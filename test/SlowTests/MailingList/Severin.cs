using System;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class Severin_null_data_time : RavenTestBase
    {
        [Fact]
        public void QueryDateCompareTest()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore => documentStore.OnBeforeQuery += 
                (sender, beforeQueryExecutedArgs) => beforeQueryExecutedArgs.QueryCustomization.WaitForNonStaleResults()
        }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { Id = "Test 1", Birthday = null });
                    session.Store(new TestDoc { Id = "Test 2", Birthday = new DateTime(2000, 1, 1) });
                    session.Store(new TestDoc { Id = "Test 3", Birthday = new DateTime(2010, 1, 1) });
                    session.Store(new TestDoc { Id = "Test 4", Birthday = new DateTime(2015, 1, 1) });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query1 = session.Query<TestDoc>().Where(x => x.Birthday == null);
                    var uery1Result = query1.ToList();
                    Assert.Equal(1, uery1Result.Count);

                    var query2 = session.Query<TestDoc>().Where(x => x.Birthday < new DateTime(2005, 1, 1));
                    var uery2Result = query2.ToList();
                    Assert.Equal(1, uery2Result.Count);

                    var query3 = session.Query<TestDoc>().Where(x => x.Birthday > new DateTime(2005, 1, 1));
                    var uery3Result = query3.ToList();
                    Assert.Equal(2, uery3Result.Count);
                    // This fails, Test 1 null values included.
                }
            }
        }

        public class TestDoc
        {
            public string Id { get; set; }

            public DateTime? Birthday { get; set; }
        }
    }
}

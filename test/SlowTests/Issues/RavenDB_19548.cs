using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19548 : RavenTestBase
    {
        public RavenDB_19548(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task TestCase()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new TestObj { Name = "Omer", Prop1 = "123456", Prop2 = "12", Birthday = new DateTime(1994, 3, 22) });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var asyncDocumentQuery = from result in session.Query<TestObj>() select result.Prop1.Length;
                    Console.WriteLine(asyncDocumentQuery);
                    Assert.Equal(6, await asyncDocumentQuery.SingleAsync());
                }


                using (var session = store.OpenAsyncSession())
                {
                    var asyncDocumentQuery = from result in session.Query<TestObj>()
                                             let ret = RavenQuery.Raw<int>("result.Prop1.length")
                                             select ret;

                    Console.WriteLine(asyncDocumentQuery);

                    var i = await asyncDocumentQuery.SingleAsync();
                    Assert.Equal(6, i);

                }

                using (var session = store.OpenAsyncSession())
                {
                    var asyncDocumentQuery = from result in session.Query<TestObj>()
                                             let ret = RavenQuery.Raw<int>("result.Prop1.length")
                                             let ret2 = RavenQuery.Raw<int>("result.Prop2.length")
                                             let x = ret + ret2
                                             select x;

                    Console.WriteLine(asyncDocumentQuery);

                    var i = await asyncDocumentQuery.SingleAsync();
                    Assert.Equal(8, i);

                }

                using (var session = store.OpenAsyncSession())
                {
                    var asyncDocumentQuery = from result in session.Query<TestObj>()
                                             let ret = RavenQuery.Raw<string>("result.Name.substr(0,3)")
                                             select ret;

                    Console.WriteLine(asyncDocumentQuery);
                    var queryResult = await asyncDocumentQuery.ToListAsync();

                    Assert.Equal("Ome", queryResult[0]);


                }

                using (var session = store.OpenAsyncSession())
                {
                    var asyncDocumentQuery = from result in session.Query<TestObj>()
                                             let ret = RavenQuery.Raw<DateTime>("new Date(Date.parse(result.Birthday))")
                                             select ret;

                    Console.WriteLine(asyncDocumentQuery);
                    var queryResult = await asyncDocumentQuery.ToListAsync();

                    Assert.Equal(new DateTime(1994, 3, 22), queryResult[0].Date);


                }
            }
        }
    }
    class TestObj
    {
        public string Id { get; set; }
        public string Prop1 { get; set; }
        public string Prop2 { get; set; }
        public DateTime Birthday { get; set; }
        public string Name { get; set; }
    }
}

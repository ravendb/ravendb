using FastTests;
using System.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using System.Threading.Tasks;
using FastTests.Server.JavaScript;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11721 : RavenTestBase
    {
        public RavenDB_11721(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void CanSwitchFromDocumentQueryToStronglyTypedProjection(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Address { Country = "Israel" }, "addresses/1");
                    s.Store(new User { Name = "Oren", AddressId = "addresses/1" });
                    s.SaveChanges();

                }

                using (var s = store.OpenSession())
                {
                    var dq = s.Advanced.DocumentQuery<User>()
                        .WhereEquals("Name", "Oren")
                        .ToQueryable();

                    var q = from u in dq
                            let address = s.Load<Address>(u.AddressId)
                            select new
                            {
                                u.Name,
                                address.Country
                            };
                    const string expected = "from 'Users' as u " +
                                            "where u.Name = $p0 " +
                                            "load u?.AddressId as address " +
                                            "select { Name : u?.Name, Country : address?.Country }";
                    for (int i = 0; i < 3; i++)
                    {
                        Assert.Equal(expected, q.ToString());
                    }

                    for (int i = 0; i < 3; i++)
                    {
                        var results = q.ToList();

                        Assert.Equal(1, results.Count);
                        Assert.Equal("Oren", results[0].Name);
                        Assert.Equal("Israel", results[0].Country);
                        s.Advanced.Clear();
                    }
                }
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public async Task AsyncCanSwitchFromDocumentQueryToStronglyTypedProjection(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var s = store.OpenAsyncSession())
                {
                    await s.StoreAsync(new Address { Country = "Israel" }, "addresses/1");
                    await s.StoreAsync(new User { Name = "Oren", AddressId = "addresses/1" });
                    await s.SaveChangesAsync();

                }

                using (var s = store.OpenAsyncSession())
                {
                    var dq = s.Advanced.AsyncDocumentQuery<User>()
                        .WhereEquals("Name", "Oren")
                        .ToQueryable();

                    var q = from u in dq
                            let address = RavenQuery.Load<Address>(u.AddressId)
                            select new
                            {
                                u.Name,
                                address.Country
                            };
                    const string expected = "from 'Users' as u " +
                                            "where u.Name = $p0 " +
                                            "load u?.AddressId as address " +
                                            "select { Name : u?.Name, Country : address?.Country }";
                    for (int i = 0; i < 3; i++)
                    {
                        Assert.Equal(expected, q.ToString());
                    }
                    for (int i = 0; i < 3; i++)
                    {
                        var results = await q.ToListAsync();

                        Assert.Equal(1, results.Count);
                        Assert.Equal("Oren", results[0].Name);
                        Assert.Equal("Israel", results[0].Country);
                        s.Advanced.Clear();
                    }


                }
            }
        }
    }
}

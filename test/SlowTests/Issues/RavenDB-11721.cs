using FastTests;
using System.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using System.Threading.Tasks;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents;

namespace SlowTests.Issues
{
    public class RavenDB_11721 : RavenTestBase
    {
        [Fact]
        public void CanSwitchFromDocumentQueryToStronglyTypedProjection()
        {
            using (var store = GetDocumentStore())
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
                    const string expected = "from Users as u " +
                                            "where u.Name = $p0 " +
                                            "load u.AddressId as address " +
                                            "select { Name : u.Name, Country : address.Country }";
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

        [Fact]
        public async Task AsyncCanSwitchFromDocumentQueryToStronglyTypedProjection()
        {
            using (var store = GetDocumentStore())
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
                    const string expected = "from Users as u " +
                                            "where u.Name = $p0 " +
                                            "load u.AddressId as address " +
                                            "select { Name : u.Name, Country : address.Country }";
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

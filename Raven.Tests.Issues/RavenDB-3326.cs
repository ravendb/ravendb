using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3326 : RavenTestBase
    {
        [Fact]
        public void streaming_and_projections_with_property_rename()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer {Name = "John", Address = "Tel Aviv"});
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Customer>(index.IndexName)
                        .Select(r => new
                        {
                            Name = r.Name,
                            OtherThanName = r.Address,
                        });

                    var enumerator = session.Advanced.Stream(query);

                    while (enumerator.MoveNext())
                    {
                        Assert.Equal("John", enumerator.Current.Document.Name);
                        Assert.Equal("Tel Aviv", enumerator.Current.Document.OtherThanName);

                        Assert.NotNull(enumerator.Current.Key);
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.TemporaryScoreValue));
                    }
                }
            }
        }

        [Fact]
        public async Task streaming_and_projections_with_property_rename_Async()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer {Name = "John", Address = "Tel Aviv"});
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    
                    var query = session.Query<Customer>(index.IndexName)
                        .Select(r => new
                        {
                            Name = r.Name,
                            OtherThanName = r.Address,
                        });

                    var enumerator = await session.Advanced.StreamAsync(query);

                    while (await enumerator.MoveNextAsync())
                    {
                        Assert.Equal("John", enumerator.Current.Document.Name);
                        Assert.Equal("Tel Aviv", enumerator.Current.Document.OtherThanName);

                        Assert.NotNull(enumerator.Current.Key);
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.TemporaryScoreValue));
                    }
                }
            }
        }

        public class Customer
        {
            public string Name { get; set; }
            public string Address { get; set; }
        }

        public class Customers_ByName : AbstractIndexCreationTask<Customer>
        {
            public Customers_ByName()
            {
                Map = customers => from customer in customers
                                   select new
                                   {
                                       customer.Name
                                   };
            }
        }
}
}
 


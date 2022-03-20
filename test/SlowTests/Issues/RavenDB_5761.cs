using System.Linq;
using Xunit.Abstractions;

using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5761 : RavenTestBase
    {
        public RavenDB_5761(ITestOutputHelper output) : base(output)
        {
        }

        private class Index1 : AbstractIndexCreationTask<User>
        {
            public Index1()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       City = LoadDocument<Address>(c.AddressId)
                                   };
            }
        }

        private class Index2 : AbstractIndexCreationTask<User, Index2.Result>
        {
            public class Result
            {
                public string City { get; set; }

                public int Count { get; set; }
            }

            public Index2()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       City = LoadDocument<Address>(c.AddressId),
                                       Count = 1
                                   };

                Reduce = results => from r in results
                                    group r by r.City into g
                                    select new
                                    {
                                        City = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }

        [Fact]
        public void StalenessShouldWorkProperlyWhenReferenceIsChanged()
        {
            using (var store = GetDocumentStore())
            {
                var index1 = new Index1();
                index1.Execute(store);

                var index2 = new Index2();
                index2.Execute(store);

                using (var session = store.OpenSession())
                {
                    var address = new Address
                    {
                        City = "New York"
                    };

                    session.Store(address);
                    session.Store(new User
                    {
                        AddressId = address.Id
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var stats = store.Maintenance.Send(new GetIndexStatisticsOperation(index1.IndexName));
                Assert.False(stats.IsStale);
                stats = store.Maintenance.Send(new GetIndexStatisticsOperation(index2.IndexName));
                Assert.False(stats.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                stats = store.Maintenance.Send(new GetIndexStatisticsOperation(index1.IndexName));
                Assert.False(stats.IsStale);
                stats = store.Maintenance.Send(new GetIndexStatisticsOperation(index2.IndexName));
                Assert.False(stats.IsStale);

                using (var session = store.OpenSession())
                {
                    var address = session.Load<Address>("addresses/1-A");
                    address.City = "Barcelona";

                    session.SaveChanges();
                }

                stats = store.Maintenance.Send(new GetIndexStatisticsOperation(index1.IndexName));
                Assert.True(stats.IsStale);
                stats = store.Maintenance.Send(new GetIndexStatisticsOperation(index2.IndexName));
                Assert.True(stats.IsStale);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                stats = store.Maintenance.Send(new GetIndexStatisticsOperation(index1.IndexName));
                Assert.False(stats.IsStale);
                stats = store.Maintenance.Send(new GetIndexStatisticsOperation(index2.IndexName));
                Assert.False(stats.IsStale);
            }
        }
    }
}

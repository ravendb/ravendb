﻿using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9587 : RavenTestBase
    {
        public RavenDB_9587(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void TimingsShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "CF" });
                    session.Store(new Company { Name = "HR" });

                    session.SaveChanges();
                }

                AssertTimings(store);
                AssertTimings(store); // there was a bug when building IndexQueryServerSide and QueryMetadataCache is used
            }

            void AssertTimings(IDocumentStore store)
            {
                using (var session = store.OpenSession())
                {
                    QueryTimings timings = null;
                    var companies = session.Query<Company>()
                        .Customize(x =>
                        {
                            x.Timings(out timings);
                            x.NoCaching();
                        })
                        .Where(x => x.Name != "HR")
                        .ToList();

                    Assert.True(timings.DurationInMs > 0);
                    Assert.NotNull(timings.Timings);
                }
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Queries.Timings;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Raven.Server.Config;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18163 : RavenTestBase
    {
        public RavenDB_18163(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CreatingARevisionManuallyAfterEnablingRevisionsForAnyCollection()
        { 
            var store = GetDocumentStore();

            // Define revision settings on Orders collection
            var configuration = new RevisionsConfiguration
            {
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    {
                        "Orders", new RevisionsCollectionConfiguration
                        {
                            PurgeOnDelete = true,
                            MinimumRevisionsToKeep = 5
                        }
                    }
                }
            };
            var result = await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

            string companyId;
            using (var session = store.OpenSession())
            {
                var company = new Company { Name = "HR" };
                session.Store(company);
                companyId = company.Id;
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var company = session.Load<Company>(companyId);

                var revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                Assert.Equal(0, revisionsCount);

                // Force revisions on a Company document
                session.Advanced.Revisions.ForceRevisionCreationFor<Company>(company);
                session.SaveChanges();

                revisionsCount = session.Advanced.Revisions.GetFor<Company>(company.Id).Count;
                Assert.Equal(1, revisionsCount); //=> this fails - should be 1 - but it is 0
            }

            store.Dispose();
        }

    }

}

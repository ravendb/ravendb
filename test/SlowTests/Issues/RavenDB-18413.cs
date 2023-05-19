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
    public class RavenDB_18413 : RavenTestBase
    {
        public RavenDB_18413(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void ToQueryableTimings(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    QueryTimings timings = null;
                    newSession.Advanced.DocumentQuery<User>()
                        .ToQueryable()
                        .Customize(x => x.Timings(out timings)).ToList();

                    Assert.NotNull(timings.Timings);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void ToQueryableTimingsOutTimings(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var newSession = store.OpenSession())
                {
                    QueryTimings timings = null;
                    newSession.Advanced.DocumentQuery<User>()
                        .Timings(out var timings2)
                        .ToQueryable()
                        .Customize(x => x.Timings(out timings)).ToList();

                    Assert.NotNull(timings.Timings);
                    Assert.Same(timings, timings2);
                }
            }
        }

    }

}

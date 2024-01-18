using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static System.Net.WebRequestMethods;

namespace SlowTests.Tests.Issues
{
    public class RavenDB_21882 : RavenTestBase
    {
        public RavenDB_21882(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Test()
        {
            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Http.Protocols)] = HttpProtocols.Http2.ToString()
                }
            });

            using var store = new DocumentStore
            {
                Urls = new[] { server.WebUrl },
                Conventions = new DocumentConventions
                {
                    ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin,
                }
            }.Initialize();
            var dbName = "test";
            var cmd = new CreateDatabaseOperation(new DatabaseRecord(dbName));
            var result = await store.Maintenance.Server.SendAsync(cmd);
            Assert.Equal(dbName,result.Name);
        }
    }
}

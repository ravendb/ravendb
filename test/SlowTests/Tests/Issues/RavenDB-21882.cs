using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Issues
{
    public class RavenDB_21882 : RavenTestBase
    {
        public RavenDB_21882(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Test()
        {
            var server = GetNewServer();

            var store = new DocumentStore
            {
                Urls = new[] { server.WebUrl },
                Conventions = new DocumentConventions
                {
                    ReadBalanceBehavior = ReadBalanceBehavior.RoundRobin,
                    HttpVersion = HttpVersion.Version20,
                    HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact,
                }
            }.Initialize();

            try
            {
                var cmd = new CreateDatabaseOperation(new DatabaseRecord("test1"));
                await store.Maintenance.Server.SendAsync(cmd);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}

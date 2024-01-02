using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Monitoring.Snmp.Objects.Database;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21874 : RavenTestBase
{
    public RavenDB_21874(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task UnusedIdWithNonBase64CharShouldThrow()
    {
        using var store = GetDocumentStore();

        var cmd = new UpdateUnusedDatabasesOperation(store.Database,
            new HashSet<string> { "6ZY2cIMkCEOzFD3CtbdH1@" }, validate: true); // @ is forbidden char

        var e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.Server.SendAsync(cmd));
        Assert.True(e.InnerException is InvalidOperationException);

        cmd = new UpdateUnusedDatabasesOperation(store.Database,
            new HashSet<string> { "6ZY2cIMkCEOzFD3CtbdH1+" }, validate: true);

        await store.Maintenance.Server.SendAsync(cmd);
    }

}



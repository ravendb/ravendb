using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;
using static SlowTests.Issues.RavenDB_9519;

namespace SlowTests.Smuggler;

public class RavenDB_16709 : RavenTestBase
{
    public RavenDB_16709(ITestOutputHelper output) : base(output) { }

    [Fact]
    public async Task ShouldProceedWhenCsvContainsEmptyColumnName()
    {
        const string userId = "user/1";
        const string name = "Lev123";

        using (var store = GetDocumentStore())
        using (var stream = new MemoryStream(Encoding.UTF8.GetBytes($"@id,Name,,,\n{userId},{name},,,")))
        using (var commands = store.Commands())
        {
            var getOperationIdCommand = new GetNextOperationIdCommand();
            await commands.RequestExecutor.ExecuteAsync(getOperationIdCommand, commands.Context);
            var operationId = getOperationIdCommand.Result;
            await commands.ExecuteAsync(new CsvImportCommand(stream, null, operationId));
            var operation = new Operation(commands.RequestExecutor, () => store.Changes(), store.Conventions, operationId);
            await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

            //Assert
            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(userId);
                Assert.Equal(name, user.Name);
            }
        }
    }
}

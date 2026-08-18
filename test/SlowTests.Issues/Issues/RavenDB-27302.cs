using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents.TasksErrors;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27302 : RavenTestBase
{
    public RavenDB_27302(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task CancellationOfEtlProcessIsNotRecordedAsUnknownError()
    {
        using (var src = GetDocumentStore())
        using (var dest = GetDocumentStore())
        {
            var database = await GetDatabase(src.Database);

            var batchCompleted = new ManualResetEventSlim();

            database.ForTestingPurposesOnly().OnEtlBatchCompleted = _ =>
            {
                batchCompleted.Set();
                throw new OperationCanceledException("The operation was canceled.");
            };

            Etl.AddEtl(src, dest, "Users", script: "loadToUsers(this);");

            using (var session = src.OpenSession())
            {
                session.Store(new User { Name = "Joe Doe" }, "users/1");
                session.SaveChanges();
            }

            Assert.True(batchCompleted.Wait(TimeSpan.FromSeconds(30)));

            var unknownErrors = await WaitForValueAsync(
                () => database.TaskErrorsStorage.ReadAllProcessErrors(TaskCategory.Etl).Count(x => x.Step == (long)TaskErrorStep.Unknown),
                expectedVal: 1, timeout: 5000, interval: 100);

            Assert.Equal(0, unknownErrors);
        }
    }
}

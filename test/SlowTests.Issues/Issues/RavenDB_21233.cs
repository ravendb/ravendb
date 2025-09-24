using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21233 : RavenTestBase
    {
        private IndexQuery q1 = new()
        {
            Query = @"
from ""Orders"" update {
    for (var i = 0; i < 100; i++){
        this.Freight = 13;
    }
}"
        };

        private IndexQuery q2 = new()
        {
            Query = @"
from index ""Orders/ByCompany""
 update {
    for (var i = 0; i < 100; i++){
        this.Total = 0;
    }
}"
        };

        private readonly IndexDefinition _indexDefinition = new()
        {
            Name = "Orders/ByCompany",
            Maps =
            {
                @"from order in docs.Orders
select new
{
    order.Company,
    Count = 1,
    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))
}"
            }
        };

        public RavenDB_21233(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ExecutePatchScriptShouldRespectIgnoreMaxStepsForScriptFlag(Options o)
        {
            Operation operation;
            QueryOperationOptions options = new();

            o.ModifyDatabaseRecord += record => record.Settings[RavenConfiguration.GetKey(x => x.Patching.MaxStepsForScript)] = "100";

            using (var store = GetDocumentStore(o))
            {

                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                /*
                 * Execute the script q1 with a greater number of steps than the default configuration
                 * IgnoreMaxStepsForScript=true
                 * should work
                 */

                options.IgnoreMaxStepsForScript = true;
                operation = await store.Operations.SendAsync(new PatchByQueryOperation(q1, options));
                await operation.WaitForCompletionAsync();

                /*
                 * Execute the script q1 with a greater number of steps than the default configuration
                 * IgnoreMaxStepsForScript=false
                 * shouldn't work
                 */
                var exception = await Assert.ThrowsAsync<Raven.Client.Exceptions.Documents.Patching.JavaScriptException>(async () =>
                {
                    options.IgnoreMaxStepsForScript = false;
                    operation = await store.Operations.SendAsync(new PatchByQueryOperation(q1, options));
                    await operation.WaitForCompletionAsync();

                });

                Assert.Contains(
                    "The maximum number of statements executed have been reached - 100. You can configure it by modifying the configuration option: 'Patching.MaxStepsForScript'.",
                    exception.ToString());


                /*
                 * Execute the script q2 (static index query) with a greater number of steps than the default configuration
                 * IgnoreMaxStepsForScript=true
                 * should work
                 */

                await store.Maintenance.SendAsync(new PutIndexesOperation(_indexDefinition));
                Indexes.WaitForIndexing(store);

                options.IgnoreMaxStepsForScript = true;
                operation = await store.Operations.SendAsync(new PatchByQueryOperation(q2, options));
                await operation.WaitForCompletionAsync();

                Indexes.WaitForIndexing(store);

                /*
                 * Execute the script q2 (static index query) with a greater number of steps than the default configuration
                 * IgnoreMaxStepsForScript=false
                 * shouldn't work
                 */

                var exception2 = await Assert.ThrowsAsync<Raven.Client.Exceptions.Documents.Patching.JavaScriptException>(async () =>
                {
                    options.IgnoreMaxStepsForScript = false;
                    operation = await store.Operations.SendAsync(new PatchByQueryOperation(q2, options));
                    await operation.WaitForCompletionAsync();
                });

                Assert.Contains(
                    "The maximum number of statements executed have been reached - 100. You can configure it by modifying the configuration option: 'Patching.MaxStepsForScript'.",
                    exception2.ToString());
            }
        }
    }
}

using System;
using System.Threading.Tasks;
using FastTests;
using Sparrow.Json;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10946 : RavenTestBase
    {
        public RavenDB_10946(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData("Lazy")]
        [InlineData("Danger")]
        public async Task CanChangeTransactionMode(string mode)
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    var response = commands.RawGetJson<BlittableJsonReaderObject>($"/admin/transactions-mode?mode={mode}");

                    foreach (var item in (BlittableJsonReaderArray)response["Environments"])
                    {
                        Assert.Equal("Mode Set Successfully", ((BlittableJsonReaderObject)item)["Result"].ToString());
                        Assert.Equal(mode, ((BlittableJsonReaderObject)item)["Mode"].ToString());
                    }

                    var documentDatabase = await GetDatabase(store.Database);

                    foreach (var env in documentDatabase.GetAllStoragesEnvironment())
                    {
                        Assert.Equal(Enum.Parse<TransactionsMode>(mode), env.Environment.Options.TransactionsMode);
                    }
                }
            }
        }
    }
}

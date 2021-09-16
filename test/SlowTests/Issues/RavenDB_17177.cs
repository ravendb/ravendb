using System;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17177 : RavenTestBase
    {
        public RavenDB_17177(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_WaitForOperation_When_TypeNameHandling_Is_None()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                {
                    CustomizeJsonSerializer = serializer => serializer.TypeNameHandling = TypeNameHandling.None
                }
            }))
            {
                var tempFile = GetTempFileName();

                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), tempFile);

                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15)); // throws
            }
        }
    }
}

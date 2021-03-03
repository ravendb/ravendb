using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14783 : RavenTestBase
    {
        public RavenDB_14783(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldWork()
        {
            var dummyDump = CreateDummyDump(1);
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var bjro = ctx.ReadObject(dummyDump, "dump"))
            await using (var ms = new MemoryStream())
            await using (var zipStream = new GZipStream(ms, CompressionMode.Compress))
            {
                await bjro.WriteJsonToAsync(zipStream);
                await zipStream.FlushAsync();
                ms.Position = 0;
                using (var store = GetDocumentStore())
                {
                    var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions
                    {
                        OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Identities | DatabaseItemType.CompareExchange,
                        MaxStepsForTransformScript = int.MaxValue,
                        TransformScript = @"
                    function sleep(milliseconds) {
                      var date = Date.now();
                      var currentDate = null;
                      do {
                        currentDate = Date.now();
                      } while (currentDate - date < milliseconds);
                    }
                    sleep(1000);
",
                    }, ms);

                    await operation.WaitForCompletionAsync();

                    var stats = store.Maintenance.Send(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                }
            }
        }

        private DynamicJsonValue CreateDummyDump(int count)
        {
            var docsList = new List<DynamicJsonValue>();

            for (int i = 0; i < count; i++)
            {
                docsList.Add(new DynamicJsonValue()
                {
                    ["π"] = Math.PI,
                    ["e"] = Math.E,
                    ["Num"] = 0xDEAD,
                    ["@metadata"] = new DynamicJsonValue()
                    {
                        ["@collection"] = $"{nameof(Math)}s",
                        ["@id"] = Guid.NewGuid().ToString()
                    }
                });
            }

            var dummyDump = new DynamicJsonValue()
            {
                ["Docs"] = new DynamicJsonArray(docsList)
            };

            return dummyDump;
        }
    }
}

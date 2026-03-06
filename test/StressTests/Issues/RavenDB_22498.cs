using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Issues;

public class RavenDB_22498_Stress : RavenTestBase
{
    public RavenDB_22498_Stress(ITestOutputHelper output) : base(output)
    {
    }

    [RavenMultiplatformTheory(RavenTestCategory.Indexes, RavenArchitecture.AllX64, NightlyBuildRequired = true)]
    [InlineData("StressTests.Data.RavenDB_22498.AutoIndexes.SlowTests.json.gz", 3319, 451, 8)]
    public Task Can_Convert_Auto_Indexes_Slow(string inputFile, int expectedCount, int expectedNotSupportedCount, int expectedErrorCount)
    {
        return Can_Convert_Auto_Indexes_Internal(inputFile, expectedCount, expectedNotSupportedCount, expectedErrorCount);
    }

    private async Task Can_Convert_Auto_Indexes_Internal(string inputFile, int expectedCount, int expectedNotSupportedCount, int expectedErrorCount)
    {
        using (var store = GetDocumentStore())
        {
            var count = 0;
            var notSupportedCount = 0;
            var errorCount = 0;

            await using (var fileStream = new GZipStream(typeof(RavenDB_22498_Stress).Assembly.GetManifestResourceStream(inputFile), CompressionMode.Decompress))
            using (var sr = new StreamReader(fileStream))
            {
                while (true)
                {
                    var s = await sr.ReadLineAsync();
                    if (string.IsNullOrWhiteSpace(s))
                        break;

                    var autoIndex = JsonConvert.DeserializeObject<AutoIndexDefinition>(s, new StringEnumConverter());

                    IndexDefinition def = null;
                    try
                    {
                        var result = AutoToStaticIndexConverter.Instance.ConvertToAbstractIndexCreationTask(autoIndex);
                        def = AutoToStaticIndexConverter.Instance.ConvertToIndexDefinition(autoIndex);
                    }
                    catch (NotSupportedException)
                    {
                        // ignore
                        notSupportedCount++;
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Failed to convert index {Environment.NewLine}{s}", e);
                    }

                    if (def == null)
                        continue;

                    try
                    {
                        await store.Maintenance.SendAsync(new PutIndexesOperation(def));
                        await store.Maintenance.SendAsync(new DeleteIndexOperation(def.Name));
                    }
                    catch (IndexCompilationException)
                    {
                        errorCount++;
                    }

                    count++;
                }
            }

            Output.WriteLine($"Converted '{count}' auto indexes. Not supported: {notSupportedCount}. Error: {errorCount}");

            Assert.Equal(expectedCount, count);
            Assert.Equal(expectedErrorCount, errorCount);
            Assert.Equal(expectedNotSupportedCount, notSupportedCount);
        }
    }
}

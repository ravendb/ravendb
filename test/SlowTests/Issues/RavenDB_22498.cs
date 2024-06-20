using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22498 : RavenTestBase
{
    public RavenDB_22498(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Can_Convert_Simple_Auto_Index()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.Query<Order>()
                    .Where(x => x.Employee == "HR" && x.ShipTo.City == "NY")
                    .Search(x => x.Company, "abc")
                    .ToListAsync();
            }

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

            var autoIndex = record.AutoIndexes.Values.First();

            var result = AutoToStaticIndexConverter.Instance.ConvertToAbstractIndexCreationTask(autoIndex, out _);
            var def = AutoToStaticIndexConverter.Instance.ConvertToIndexDefinition(autoIndex);

            await store.Maintenance.SendAsync(new PutIndexesOperation(def));
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [InlineData("SlowTests.Data.RavenDB_22498.AutoIndexes.FastTests.json.gz")]
    public async Task Can_Convert_Auto_Indexes(string inputFile)
    {
        using (var store = GetDocumentStore())
        {
            await using (var fileStream = new GZipStream(typeof(RavenDB_22498).Assembly.GetManifestResourceStream(inputFile), CompressionMode.Decompress))
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
                        var result = AutoToStaticIndexConverter.Instance.ConvertToAbstractIndexCreationTask(autoIndex, out _);
                        def = AutoToStaticIndexConverter.Instance.ConvertToIndexDefinition(autoIndex);
                    }
                    catch (NotSupportedException)
                    {
                        // ignore
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Failed to convert index {Environment.NewLine}{s}", e);
                    }

                    if (def == null) 
                        continue;

                    await store.Maintenance.SendAsync(new PutIndexesOperation(def));
                    await store.Maintenance.SendAsync(new DeleteIndexOperation(def.Name));
                }
            }
        }
    }


}



using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Sparrow.Json;
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

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task Can_Convert_Simple_Auto_Map_Index()
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
            await store.Maintenance.SendAsync(new DeleteIndexOperation(def.Name));

            using (var session = store.OpenAsyncSession())
            {
                var command = new ConvertAutoIndexCommand(autoIndex.Name);
                await store.GetRequestExecutor().ExecuteAsync(command, session.Advanced.Context);

                var def2 = command.Result;
                Assert.Equal(IndexDefinitionCompareDifferences.None, def.Compare(def2));
            }
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task Can_Convert_Simple_Auto_Map_Reduce_Index()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.Query<Order>()
                    .GroupBy(x => x.Company)
                    .Select(x => new
                    {
                        Count = x.Count(),
                        Company = x.Key
                    }).ToListAsync();
            }

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

            var autoIndex = record.AutoIndexes.Values.First();

            var result = AutoToStaticIndexConverter.Instance.ConvertToAbstractIndexCreationTask(autoIndex, out _);
            var def = AutoToStaticIndexConverter.Instance.ConvertToIndexDefinition(autoIndex);

            await store.Maintenance.SendAsync(new PutIndexesOperation(def));
            await store.Maintenance.SendAsync(new DeleteIndexOperation(def.Name));

            using (var session = store.OpenAsyncSession())
            {
                var command = new ConvertAutoIndexCommand(autoIndex.Name);
                await store.GetRequestExecutor().ExecuteAsync(command, session.Advanced.Context);

                var def2 = command.Result;
                Assert.Equal(IndexDefinitionCompareDifferences.None, def.Compare(def2));
            }
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [InlineData("SlowTests.Data.RavenDB_22498.AutoIndexes.FastTests.json.gz")]
    [InlineData("SlowTests.Data.RavenDB_22498.AutoIndexes.SlowTests.json.gz")]
    public async Task Can_Convert_Auto_Indexes(string inputFile)
    {
        using (var store = GetDocumentStore())
        {
            var count = 0;

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

                    count++;
                }
            }

            Output.WriteLine($"Converted '{count}' auto indexes");
        }
    }

    private class ConvertAutoIndexCommand : RavenCommand<IndexDefinition>
    {
        private readonly string _name;
        public override bool IsReadRequest => false;

        public ConvertAutoIndexCommand(string name)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/indexes/auto/convert?name={Uri.EscapeDataString(_name)}&type=export";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            response.TryGet("Indexes", out BlittableJsonReaderArray indexes);

            Result = JsonDeserializationClient.IndexDefinition((BlittableJsonReaderObject)indexes[0]);
        }
    }
}

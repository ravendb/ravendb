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

            var result = AutoToStaticIndexConverter.Instance.ConvertToAbstractIndexCreationTask(autoIndex);

            RavenTestHelper.AssertEqualRespectingNewLines("""
                                                          public class Index_Orders_ByEmployeeAndSearch_Company_AndShipTo_City : Raven.Client.Documents.Indexes.AbstractIndexCreationTask<Order>
                                                          {
                                                              public Index_Orders_ByEmployeeAndSearch_Company_AndShipTo_City()
                                                              {
                                                                  Map = items => from item in items
                                                                                 select new
                                                                                 {
                                                                                     Company = item.Company,
                                                                                     Company_Search = item.Company,
                                                                                     Employee = item.Employee,
                                                                                     ShipTo_City = item.ShipTo.City,
                                                                                 };
                                                          
                                                                  Index("Company_Search", Raven.Client.Documents.Indexes.FieldIndexing.Search);
                                                              }
                                                          }
                                                          
                                                          """, result);

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
    public async Task Can_Convert_Simple_Auto_Map_Index_With_Metadata()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.Advanced.AsyncDocumentQuery<Order>()
                    .WhereEquals("@metadata.'Is-Nice'", true)
                    .ToListAsync();
            }

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

            var autoIndex = record.AutoIndexes.Values.First();

            var result = AutoToStaticIndexConverter.Instance.ConvertToAbstractIndexCreationTask(autoIndex);

            RavenTestHelper.AssertEqualRespectingNewLines("""
                                                          public class Index_Orders_By_metadata_Is_Nice : Raven.Client.Documents.Indexes.AbstractIndexCreationTask<Order>
                                                          {
                                                              public Index_Orders_By_metadata_Is_Nice()
                                                              {
                                                                  Map = items => from item in items
                                                                                 select new
                                                                                 {
                                                                                     metadata_Is_Nice = MetadataFor(item)["Is-Nice"],
                                                                                 };
                                                          
                                                              }
                                                          }

                                                          """, result);

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
    public async Task Can_Convert_Simple_Auto_Map_Index_With_Empty_Collection()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.Advanced.AsyncRawQuery<object>("from \"@empty\" where Coll = 'a'")
                    .ToListAsync();
            }

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

            var autoIndex = record.AutoIndexes.Values.First();

            var result = AutoToStaticIndexConverter.Instance.ConvertToAbstractIndexCreationTask(autoIndex);

            RavenTestHelper.AssertEqualRespectingNewLines("""
                                                          public class Index__empty_ByColl : Raven.Client.Documents.Indexes.AbstractIndexCreationTask<object>
                                                          {
                                                              public Index__empty_ByColl()
                                                              {
                                                                  Map = items => from item in items
                                                                                 select new
                                                                                 {
                                                                                     Coll = item.Coll,
                                                                                 };
                                                          
                                                              }
                                                          }

                                                          """, result);

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

            var result = AutoToStaticIndexConverter.Instance.ConvertToAbstractIndexCreationTask(autoIndex);

            RavenTestHelper.AssertEqualRespectingNewLines("""
                                                          public class Index_Orders_ByCountReducedByCompany : Raven.Client.Documents.Indexes.AbstractIndexCreationTask<Order, Index_Orders_ByCountReducedByCompany.Result>
                                                          {
                                                              public Index_Orders_ByCountReducedByCompany()
                                                              {
                                                                  Map = items => from item in items
                                                                                 select new
                                                                                 {
                                                                                     Count = 1,
                                                                                     Company = item.Company,
                                                                                 };
                                                          
                                                                  Reduce = results => from result in results
                                                                                      group result by new { result.Company } into g
                                                                                      select new
                                                                                      {
                                                                                          Count = g.Sum(x => x.Count),
                                                                                          Company = g.Key.Company,
                                                                                      };
                                                          
                                                              }
                                                          
                                                              public class Result
                                                              {
                                                                  public int Count { get; set; }
                                                                  public object Company { get; set; }
                                                              }
                                                          }

                                                          """, result);

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
    [InlineData("SlowTests.Data.RavenDB_22498.AutoIndexes.FastTests.json.gz", 678, 10, 0)]
    public Task Can_Convert_Auto_Indexes_Fast(string inputFile, int expectedCount, int expectedNotSupportedCount, int expectedErrorCount)
    {
        return Can_Convert_Auto_Indexes_Internal(inputFile, expectedCount, expectedNotSupportedCount, expectedErrorCount);
    }

    [RavenTheory(RavenTestCategory.Indexes, NightlyBuildRequired = true)]
    [InlineData("SlowTests.Data.RavenDB_22498.AutoIndexes.SlowTests.json.gz", 3319, 451, 8)]
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
                    catch
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
            url = $"{node.Url}/databases/{node.Database}/indexes/auto/convert?name={Uri.EscapeDataString(_name)}&outputType=json";

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

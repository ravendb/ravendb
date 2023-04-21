using System;
using System.Collections.Generic;
using System.Net.Http;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Indexes.Test;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_11097 : RavenTestBase
{
    public RavenDB_11097(ITestOutputHelper output) : base(output)
    {
    }
    
    private class PutTestIndexCommand : RavenCommand<object>
    {
        private readonly Payload _payload;
        public PutTestIndexCommand(Payload payload)
        {
            _payload = payload;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/indexes/test";

            var payloadJson = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_payload, ctx);

            var documentConventions = new DocumentConventions();

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await ctx.WriteAsync(stream, payloadJson);
                }, documentConventions)
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = response;
        }

        public override bool IsReadRequest => true;
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void DocumentLinqMap(Options options) => TestMapIndexOnDocuments(options, 
        new Payload()
        {
            IndexDefinition = new IndexDefinition()
            { 
                Name = "CoolLinqMapIndex", 
                Maps = new HashSet<string>
                {
                    "from dto in docs.Dtos select new { Name = dto.Name, Age = dto.Age }"
                }
            },
            Query = new QueryClass()
            {
                Query = "from index 'CoolLinqMapIndex' select Age"
            }
        });
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void DocumentJsMap(Options options) => TestMapIndexOnDocuments(options, 
        new Payload()
        {
            IndexDefinition = new IndexDefinition()
            {
                Name = "CoolJsMapIndex",
                Maps = new HashSet<string>
                {
                    "map('Dtos', (dto) => { return { Name: dto.Name, Age: dto.Age }; })"
                }
            },
            Query = new QueryClass()
            {
                Query = "from index 'CoolJsMapIndex' select Age"
            }
        });
    
    private void TestMapIndexOnDocuments(Options options, Payload payload)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Name = "Name1", Age = 21 };
                var dto2 = new Dto() { Name = "Name2", Age = 37 };

                session.Store(dto1);
                session.Store(dto2);

                session.SaveChanges();
            }
            
            using (var commands = store.Commands())
            {
                var cmd = new PutTestIndexCommand(payload);
                commands.Execute(cmd);
                
                var res = cmd.Result as BlittableJsonReaderObject;
                
                Assert.NotNull(res);
                
                res.TryGet(nameof(TestIndexResult.IndexEntries), out BlittableJsonReaderArray indexEntries);
                res.TryGet(nameof(TestIndexResult.QueryResults), out BlittableJsonReaderArray queryResults);
                res.TryGet(nameof(TestIndexResult.MapResults), out BlittableJsonReaderArray mapResults);
                res.TryGet(nameof(TestIndexResult.ReduceResults), out BlittableJsonReaderArray reduceResults);

                var indexEntriesObjectList = JsonConvert.DeserializeObject<List<Dto>>(indexEntries.ToString());
                var queryResultsObjectList = JsonConvert.DeserializeObject<List<Dto>>(queryResults.ToString());
                var mapResultsObjectList = JsonConvert.DeserializeObject<List<Dto>>(mapResults.ToString());
                var reduceResultsObjectList = JsonConvert.DeserializeObject<List<Dto>>(reduceResults.ToString());

                Assert.Equal("name1", indexEntriesObjectList[0].Name);
                Assert.Equal("name2", indexEntriesObjectList[1].Name);
                Assert.Equal(21, indexEntriesObjectList[0].Age);
                Assert.Equal(37, indexEntriesObjectList[1].Age);
                
                Assert.Null(queryResultsObjectList[0].Name);
                Assert.Null(queryResultsObjectList[1].Name);
                Assert.Equal(21, queryResultsObjectList[0].Age);
                Assert.Equal(37, queryResultsObjectList[1].Age);
                
                Assert.Equal("Name1", mapResultsObjectList[0].Name);
                Assert.Equal("Name2", mapResultsObjectList[1].Name);
                Assert.Equal(21, mapResultsObjectList[0].Age);
                Assert.Equal(37, mapResultsObjectList[1].Age);
                
                Assert.Empty(reduceResultsObjectList);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    private void DocumentJsMapWithoutQueryAndIndexName()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Name = "Name1", Age = 21 };
                var dto2 = new Dto() { Name = "Name2", Age = 37 };

                session.Store(dto1);
                session.Store(dto2);

                session.SaveChanges();
            }
            
            using (var commands = store.Commands())
            {
                var payload = new Payload()
                {
                    IndexDefinition = new IndexDefinition()
                    {
                        Maps = new HashSet<string> { "map('Dtos', (dto) => { return { Name: dto.Name, Age: dto.Age }; })" }
                    }
                };
                
                var cmd = new PutTestIndexCommand(payload);
                commands.Execute(cmd);
                
                var res = cmd.Result as BlittableJsonReaderObject;
                
                Assert.NotNull(res);
                
                res.TryGet(nameof(TestIndexResult.IndexEntries), out BlittableJsonReaderArray indexEntries);
                res.TryGet(nameof(TestIndexResult.QueryResults), out BlittableJsonReaderArray queryResults);
                res.TryGet(nameof(TestIndexResult.MapResults), out BlittableJsonReaderArray mapResults);
                res.TryGet(nameof(TestIndexResult.ReduceResults), out BlittableJsonReaderArray reduceResults);

                var indexEntriesObjectList = JsonConvert.DeserializeObject<List<Dto>>(indexEntries.ToString());
                var queryResultsObjectList = JsonConvert.DeserializeObject<List<Dto>>(queryResults.ToString());
                var mapResultsObjectList = JsonConvert.DeserializeObject<List<Dto>>(mapResults.ToString());
                var reduceResultsObjectList = JsonConvert.DeserializeObject<List<Dto>>(reduceResults.ToString());
                
                Assert.Equal("name1", indexEntriesObjectList[0].Name);
                Assert.Equal("name2", indexEntriesObjectList[1].Name);
                Assert.Equal(21, indexEntriesObjectList[0].Age);
                Assert.Equal(37, indexEntriesObjectList[1].Age);
                
                Assert.Equal("Name1", queryResultsObjectList[0].Name);
                Assert.Equal("Name2", queryResultsObjectList[1].Name);
                Assert.Equal(21, queryResultsObjectList[0].Age);
                Assert.Equal(37, queryResultsObjectList[1].Age);
                
                Assert.Equal("Name1", mapResultsObjectList[0].Name);
                Assert.Equal("Name2", mapResultsObjectList[1].Name);
                Assert.Equal(21, mapResultsObjectList[0].Age);
                Assert.Equal(37, mapResultsObjectList[1].Age);
                
                Assert.Empty(reduceResultsObjectList);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void DocumentJsMapReduce(Options options) => TestMapReduceIndexOnDocuments(options, 
        new Payload()
        {
            IndexDefinition = new IndexDefinition()
            {
                Name = "CoolJsMapReduceIndex",
                Maps = new HashSet<string>
                {
                    "map('Dtos', (dto) => { return { Name: dto.Name, Age: dto.Age, Count: 1 }; })"
                },
                Reduce = "groupBy(x => ({ Name: x.Name })).aggregate(g => { return { Name: g.key.Name, Count: g.values.reduce((count, val) => val.Count + count, 0), Age: g.values.reduce((age, val) => val.Age + age, 0) }; })"
            },
            Query = new QueryClass()
            {
                Query = "from index 'CoolJsMapReduceIndex' select Count"
            }
        });
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void DocumentLinqMapReduce(Options options) => TestMapReduceIndexOnDocuments(options, 
        new Payload()
        {
            IndexDefinition = new IndexDefinition()
            {
                Name = "CoolLinqMapReduceIndex",
                Maps = new HashSet<string>
                {
                    "from dto in docs.Dtos select new { Name = dto.Name, Age = dto.Age, Count = 1 }"
                },
                Reduce = "from result in results group result by new { result.Name } into g select new { Name = g.Key.Name, Count = g.Sum(x => x.Count), Age = g.Sum(x => x.Age) }"
            },
            Query = new QueryClass()
            {
                Query = "from index 'CoolLinqMapReduceIndex' select Count"
            }
        });
    
    private void TestMapReduceIndexOnDocuments(Options options, Payload payload)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Name = "Name1", Age = 21 };
                var dto2 = new Dto() { Name = "Name2", Age = 37 };
                var dto3 = new Dto() { Name = "Name1", Age = 55 };
                
                session.Store(dto1);
                session.Store(dto2);
                session.Store(dto3);

                session.SaveChanges();
            }
            
            using (var commands = store.Commands())
            {
                var cmd = new PutTestIndexCommand(payload);
                commands.Execute(cmd);
                
                var res = cmd.Result as BlittableJsonReaderObject;
                
                Assert.NotNull(res);
                
                res.TryGet(nameof(TestIndexResult.IndexEntries), out BlittableJsonReaderArray indexEntries);
                res.TryGet(nameof(TestIndexResult.QueryResults), out BlittableJsonReaderArray queryResults);
                res.TryGet(nameof(TestIndexResult.MapResults), out BlittableJsonReaderArray mapResults);
                res.TryGet(nameof(TestIndexResult.ReduceResults), out BlittableJsonReaderArray reduceResults);
                
                var indexEntriesObjectList = JsonConvert.DeserializeObject<List<Dto>>(indexEntries.ToString());
                var queryResultsObjectList = JsonConvert.DeserializeObject<List<Dto>>(queryResults.ToString());
                var mapResultsObjectList = JsonConvert.DeserializeObject<List<Dto>>(mapResults.ToString());
                var reduceResultsObjectList = JsonConvert.DeserializeObject<List<Dto>>(reduceResults.ToString());
                
                Assert.Equal("name1", indexEntriesObjectList[0].Name);
                Assert.Equal("name2", indexEntriesObjectList[1].Name);
                Assert.Equal(76, indexEntriesObjectList[0].Age);
                Assert.Equal(37, indexEntriesObjectList[1].Age);

                Assert.Equal(2, queryResultsObjectList[0].Count);
                Assert.Equal(1, queryResultsObjectList[1].Count);
                
                Assert.Equal("Name1", mapResultsObjectList[0].Name);
                Assert.Equal("Name2", mapResultsObjectList[1].Name);
                Assert.Equal("Name1", mapResultsObjectList[2].Name);
                Assert.Equal(21, mapResultsObjectList[0].Age);
                Assert.Equal(37, mapResultsObjectList[1].Age);
                Assert.Equal(55, mapResultsObjectList[2].Age);
                Assert.Equal(1, mapResultsObjectList[0].Count);
                Assert.Equal(1, mapResultsObjectList[1].Count);
                Assert.Equal(1, mapResultsObjectList[2].Count);
                
                Assert.Equal("Name1", reduceResultsObjectList[0].Name);
                Assert.Equal("Name2", reduceResultsObjectList[1].Name);
                Assert.Equal(76, reduceResultsObjectList[0].Age);
                Assert.Equal(37, reduceResultsObjectList[1].Age);
                Assert.Equal(2, reduceResultsObjectList[0].Count);
                Assert.Equal(1, reduceResultsObjectList[1].Count);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    private void DocumentLinqMultimapReduce()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Name = "Name1", Age = 21 };
                var dto2 = new Dto() { Name = "Name2", Age = 37 };

                session.Store(dto1);
                session.Store(dto2);

                var c1 = new Cat() { Name = "Name1", Age = 4 };
                var c2 = new Cat() { Name = "Kitty", Age = 3 };
                
                session.Store(c1);
                session.Store(c2);
                
                session.SaveChanges();
            }
            
            using (var commands = store.Commands())
            {
                var payload = new Payload()
                {
                    IndexDefinition = new IndexDefinition()
                    {
                        Name = "CoolLinqMapReduceIndex",
                        Maps = new HashSet<string>
                        {
                            "from dto in docs.Dtos select new { Name = dto.Name, Age = dto.Age, Count = 1 }",
                            "from cat in docs.Cats select new { Name = cat.Name, Age = cat.Age, Count = 1 }"
                        },
                        Reduce =
                            "from result in results group result by new { result.Name } into g select new { Name = g.Key.Name, Count = g.Sum(x => x.Count), Age = g.Sum(x => x.Age) }"
                    },
                    Query = new QueryClass() { Query = "from index 'CoolLinqMapReduceIndex' select Count" }
                };
                
                var cmd = new PutTestIndexCommand(payload);
                commands.Execute(cmd);
                
                var res = cmd.Result as BlittableJsonReaderObject;
                
                Assert.NotNull(res);
                
                res.TryGet(nameof(TestIndexResult.IndexEntries), out BlittableJsonReaderArray indexEntries);
                res.TryGet(nameof(TestIndexResult.QueryResults), out BlittableJsonReaderArray queryResults);
                res.TryGet(nameof(TestIndexResult.MapResults), out BlittableJsonReaderArray mapResults);
                res.TryGet(nameof(TestIndexResult.ReduceResults), out BlittableJsonReaderArray reduceResults);
                
                var indexEntriesObjectList = JsonConvert.DeserializeObject<List<Dto>>(indexEntries.ToString());
                var queryResultsObjectList = JsonConvert.DeserializeObject<List<Dto>>(queryResults.ToString());
                var mapResultsObjectList = JsonConvert.DeserializeObject<List<Dto>>(mapResults.ToString());
                var reduceResultsObjectList = JsonConvert.DeserializeObject<List<Dto>>(reduceResults.ToString());
                
                Assert.Equal("name1", indexEntriesObjectList[0].Name);
                Assert.Equal("name2", indexEntriesObjectList[1].Name);
                Assert.Equal("kitty", indexEntriesObjectList[2].Name);
                Assert.Equal(25, indexEntriesObjectList[0].Age);
                Assert.Equal(37, indexEntriesObjectList[1].Age);
                Assert.Equal(3, indexEntriesObjectList[2].Age);
                
                Assert.Equal(2, queryResultsObjectList[0].Count);
                Assert.Equal(1, queryResultsObjectList[1].Count);
                Assert.Equal(1, queryResultsObjectList[2].Count);
                
                Assert.Equal("Name1", mapResultsObjectList[0].Name);
                Assert.Equal("Name2", mapResultsObjectList[1].Name);
                Assert.Equal("Name1", mapResultsObjectList[2].Name);
                Assert.Equal("Kitty", mapResultsObjectList[3].Name);
                Assert.Equal(21, mapResultsObjectList[0].Age);
                Assert.Equal(37, mapResultsObjectList[1].Age);
                Assert.Equal(4, mapResultsObjectList[2].Age);
                Assert.Equal(3, mapResultsObjectList[3].Age);
                Assert.Equal(1, mapResultsObjectList[0].Count);
                Assert.Equal(1, mapResultsObjectList[1].Count);
                Assert.Equal(1, mapResultsObjectList[2].Count);
                Assert.Equal(1, mapResultsObjectList[3].Count);
                
                Assert.Equal("Name1", reduceResultsObjectList[0].Name);
                Assert.Equal("Name2", reduceResultsObjectList[1].Name);
                Assert.Equal("Kitty", reduceResultsObjectList[2].Name);
                Assert.Equal(25, reduceResultsObjectList[0].Age);
                Assert.Equal(37, reduceResultsObjectList[1].Age);
                Assert.Equal(3, reduceResultsObjectList[2].Age);
                Assert.Equal(2, reduceResultsObjectList[0].Count);
                Assert.Equal(1, reduceResultsObjectList[1].Count);
                Assert.Equal(1, reduceResultsObjectList[2].Count);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    private void TimeSeriesLinqMap(Options options) => TestMapIndexOnTimeSeries(options,
        new Payload()
        {
            IndexDefinition = new IndexDefinition()
            {
                Name = "CoolLinqMapIndex",
                Maps = new HashSet<string>
                {
                    "from ts in timeSeries.Dtos.HeartRates from entry in ts.Entries select new { Tag = entry.Tag, FirstValue = entry.Values[0] }"
                }
            },
            Query = new QueryClass()
            {
                Query = "from index 'CoolLinqMapIndex' select Tag"
            }
        });
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    private void TimeSeriesJsMap(Options options) => TestMapIndexOnTimeSeries(options,
        new Payload()
        {
            IndexDefinition = new IndexDefinition()
            {
                Name = "CoolJsMapIndex",
                Maps = new HashSet<string>
                {
                    "timeSeries.map('Dtos', function (ts) { return ts.Entries.map(entry => ({ Tag: entry.Tag, FirstValue: entry.Values[0] })); })"
                }
            },
            Query = new QueryClass()
            {
                Query = "from index 'CoolJsMapIndex' select Tag"
            }
        });
    
    private void TestMapIndexOnTimeSeries(Options options, Payload payload)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var baseline = DateTime.Now;
                
                var dto1 = new Dto() { Name = "Name1", Age = 21 };
                var dto2 = new Dto() { Name = "Name2", Age = 37 };
                var dto3 = new Dto() { Name = "Name3", Age = 55 };
                
                session.Store(dto1);
                session.Store(dto2);
                session.Store(dto3);
                
                ISessionDocumentTimeSeries tsf1 = session.TimeSeriesFor(dto1.Id, "HeartRates");
                ISessionDocumentTimeSeries tsf2 = session.TimeSeriesFor(dto2.Id, "HeartRates");
                ISessionDocumentTimeSeries tsf3 = session.TimeSeriesFor(dto3.Id, "HeartRates");
                
                tsf1.Append(baseline.AddSeconds(1), new[] { 67d }, "tag/1");
                tsf2.Append(baseline.AddSeconds(2), new[] { 68d }, "tag/2");
                tsf3.Append(baseline.AddSeconds(3), new[] { 69d }, "tag/3");
                
                session.SaveChanges();
            }
            
            using (var commands = store.Commands())
            {
                var cmd = new PutTestIndexCommand(payload);
                commands.Execute(cmd);
                
                var res = cmd.Result as BlittableJsonReaderObject;
                
                Assert.NotNull(res);
                
                res.TryGet(nameof(TestIndexResult.IndexEntries), out BlittableJsonReaderArray indexEntries);
                res.TryGet(nameof(TestIndexResult.QueryResults), out BlittableJsonReaderArray queryResults);
                res.TryGet(nameof(TestIndexResult.MapResults), out BlittableJsonReaderArray mapResults);
                res.TryGet(nameof(TestIndexResult.ReduceResults), out BlittableJsonReaderArray reduceResults);
                
                var indexEntriesObjectList = JsonConvert.DeserializeObject<List<DtoForTS>>(indexEntries.ToString());
                var queryResultsObjectList = JsonConvert.DeserializeObject<List<DtoForTS>>(queryResults.ToString());
                var mapResultsObjectList = JsonConvert.DeserializeObject<List<DtoForTS>>(mapResults.ToString());
                var reduceResultsObjectList = JsonConvert.DeserializeObject<List<DtoForTS>>(reduceResults.ToString());
                
                Assert.Equal("tag/1", indexEntriesObjectList[0].Tag);
                Assert.Equal("tag/2", indexEntriesObjectList[1].Tag);
                Assert.Equal("tag/3", indexEntriesObjectList[2].Tag);
                Assert.Equal(67, indexEntriesObjectList[0].FirstValue);
                Assert.Equal(68, indexEntriesObjectList[1].FirstValue);
                Assert.Equal(69, indexEntriesObjectList[2].FirstValue);
                
                Assert.Equal("tag/1", queryResultsObjectList[0].Tag);
                Assert.Equal("tag/2", queryResultsObjectList[1].Tag);
                Assert.Equal("tag/3", queryResultsObjectList[2].Tag);
                
                Assert.Equal("tag/1", mapResultsObjectList[0].Tag);
                Assert.Equal("tag/2", mapResultsObjectList[1].Tag);
                Assert.Equal("tag/3", mapResultsObjectList[2].Tag);
                Assert.Equal(67, mapResultsObjectList[0].FirstValue);
                Assert.Equal(68, mapResultsObjectList[1].FirstValue);
                Assert.Equal(69, mapResultsObjectList[2].FirstValue);
                
                Assert.Empty(reduceResultsObjectList);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    private void TimeSeriesLinqMapReduce(Options options) => TestMapReduceIndexOnTimeSeries(options,
        new Payload()
        {
            IndexDefinition = new IndexDefinition()
            {
                Name = "CoolLinqMapReduceIndex",
                Maps = new HashSet<string>
                {
                    "from ts in timeSeries.Dtos.HeartRates from entry in ts.Entries select new { Tag = entry.Tag, FirstValue = entry.Values[0] }"
                },
                Reduce = "from result in results group result by new { result.Tag } into g select new { Tag = g.Key.Tag, FirstValue = g.Sum(x => x.FirstValue) }"
            },
            Query = new QueryClass()
            {
                Query = "from index 'CoolLinqMapReduceIndex'"
            }
        });
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    private void TimeSeriesJsMapReduce(Options options) => TestMapReduceIndexOnTimeSeries(options,
        new Payload()
        {
            IndexDefinition = new IndexDefinition()
            {
                Name = "CoolJsMapReduceIndex",
                Maps = new HashSet<string>
                {
                    "timeSeries.map('Dtos', function (ts) { return ts.Entries.map(entry => ({ Tag: entry.Tag, FirstValue: entry.Values[0] })); })"
                },
                Reduce = "groupBy(x => ({ Tag: x.Tag })).aggregate(g => { return { Tag: g.key.Tag, FirstValue: g.values.reduce((count, val) => val.FirstValue + count, 0) } })"
            },
            Query = new QueryClass()
            {
                Query = "from index 'CoolJsMapReduceIndex'"
            }
        });
    
    private void TestMapReduceIndexOnTimeSeries(Options options, Payload payload)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var baseline = DateTime.Now;
                
                var dto1 = new Dto() { Name = "Name1", Age = 21 };
                var dto2 = new Dto() { Name = "Name2", Age = 37 };
                var dto3 = new Dto() { Name = "Name3", Age = 55 };
                
                session.Store(dto1);
                session.Store(dto2);
                session.Store(dto3);
                
                ISessionDocumentTimeSeries tsf1 = session.TimeSeriesFor(dto1.Id, "HeartRates");
                ISessionDocumentTimeSeries tsf2 = session.TimeSeriesFor(dto2.Id, "HeartRates");
                ISessionDocumentTimeSeries tsf3 = session.TimeSeriesFor(dto3.Id, "HeartRates");
                
                tsf1.Append(baseline.AddSeconds(1), new[] { 67d }, "tag/1");
                tsf2.Append(baseline.AddSeconds(2), new[] { 68d }, "tag/2");
                tsf3.Append(baseline.AddSeconds(3), new[] { 69d }, "tag/1");
                
                session.SaveChanges();
            }
            
            using (var commands = store.Commands())
            {
                var cmd = new PutTestIndexCommand(payload);
                commands.Execute(cmd);
                
                var res = cmd.Result as BlittableJsonReaderObject;
                
                Assert.NotNull(res);
                
                res.TryGet(nameof(TestIndexResult.IndexEntries), out BlittableJsonReaderArray indexEntries);
                res.TryGet(nameof(TestIndexResult.QueryResults), out BlittableJsonReaderArray queryResults);
                res.TryGet(nameof(TestIndexResult.MapResults), out BlittableJsonReaderArray mapResults);
                res.TryGet(nameof(TestIndexResult.ReduceResults), out BlittableJsonReaderArray reduceResults);
                
                var indexEntriesObjectList = JsonConvert.DeserializeObject<List<DtoForTS>>(indexEntries.ToString());
                var queryResultsObjectList = JsonConvert.DeserializeObject<List<DtoForTS>>(queryResults.ToString());
                var mapResultsObjectList = JsonConvert.DeserializeObject<List<DtoForTS>>(mapResults.ToString());
                var reduceResultsObjectList = JsonConvert.DeserializeObject<List<DtoForTS>>(reduceResults.ToString());
                
                Assert.Equal("tag/1", indexEntriesObjectList[0].Tag);
                Assert.Equal("tag/2", indexEntriesObjectList[1].Tag);
                Assert.Equal(136, indexEntriesObjectList[0].FirstValue);
                Assert.Equal(68, indexEntriesObjectList[1].FirstValue);
                
                Assert.Equal("tag/1", queryResultsObjectList[0].Tag);
                Assert.Equal("tag/2", queryResultsObjectList[1].Tag);
                Assert.Equal(136, queryResultsObjectList[0].FirstValue);
                Assert.Equal(68, queryResultsObjectList[1].FirstValue);

                Assert.Equal("tag/1", mapResultsObjectList[0].Tag);
                Assert.Equal("tag/2", mapResultsObjectList[1].Tag);
                Assert.Equal("tag/1", mapResultsObjectList[2].Tag);
                Assert.Equal(67, mapResultsObjectList[0].FirstValue);
                Assert.Equal(68, mapResultsObjectList[1].FirstValue);
                Assert.Equal(69, mapResultsObjectList[2].FirstValue);
                
                Assert.Equal("tag/1", reduceResultsObjectList[0].Tag);
                Assert.Equal("tag/2", reduceResultsObjectList[1].Tag);
                Assert.Equal(136, reduceResultsObjectList[0].FirstValue);
                Assert.Equal(68, reduceResultsObjectList[1].FirstValue);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    private void CountersLinqMap(Options options) => TestMapIndexOnCounters(options,
        new Payload()
        {
            IndexDefinition = new IndexDefinition()
            {
                Name = "CoolLinqMapIndex",
                Maps = new HashSet<string>
                {
                    "from counter in counters.Dtos.Likes select new { Value = counter.Value }"
                }
            },
            Query = new QueryClass()
            {
                Query = "from index 'CoolLinqMapIndex'"
            }
        });
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    private void CountersJsMap(Options options) => TestMapIndexOnCounters(options,
        new Payload()
        {
            IndexDefinition = new IndexDefinition()
            {
                Name = "CoolJsMapIndex",
                Maps = new HashSet<string>
                {
                    "counters.map('Dtos', 'Likes', function (counter) { return { Value: counter.Value } })"
                }
            },
            Query = new QueryClass()
            {
                Query = "from index 'CoolJsMapIndex'"
            }
        });
    
    private void TestMapIndexOnCounters(Options options, Payload payload, bool isJs = false)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Name = "Name1", Age = 21 };
                var dto2 = new Dto() { Name = "Name2", Age = 37 };
                var dto3 = new Dto() { Name = "Name3", Age = 55 };
                
                session.Store(dto1);
                session.Store(dto2);
                session.Store(dto3);
                
                var c1 = session.CountersFor(dto1.Id);
                var c2 = session.CountersFor(dto2.Id);
                var c3 = session.CountersFor(dto3.Id);
                
                c1.Increment("Likes");
                c2.Increment("Likes", 21);
                c3.Increment("Likes", 37);
                
                session.SaveChanges();
            }
            
            using (var commands = store.Commands())
            {
                var cmd = new PutTestIndexCommand(payload);
                commands.Execute(cmd);
                
                var res = cmd.Result as BlittableJsonReaderObject;
                
                Assert.NotNull(res);
                
                res.TryGet(nameof(TestIndexResult.IndexEntries), out BlittableJsonReaderArray indexEntries);
                res.TryGet(nameof(TestIndexResult.QueryResults), out BlittableJsonReaderArray queryResults);
                res.TryGet(nameof(TestIndexResult.MapResults), out BlittableJsonReaderArray mapResults);
                res.TryGet(nameof(TestIndexResult.ReduceResults), out BlittableJsonReaderArray reduceResults);
                
                var indexEntriesObjectList = JsonConvert.DeserializeObject<List<DtoForCounters>>(indexEntries.ToString());
                var queryResultsObjectList = JsonConvert.DeserializeObject<List<DtoForCounters>>(queryResults.ToString());
                var mapResultsObjectList = JsonConvert.DeserializeObject<List<DtoForCounters>>(mapResults.ToString());
                var reduceResultsObjectList = JsonConvert.DeserializeObject<List<DtoForCounters>>(reduceResults.ToString());
                
                Assert.Null(indexEntriesObjectList[0].Name);
                Assert.Null(indexEntriesObjectList[1].Name);
                Assert.Null(indexEntriesObjectList[2].Name);
                Assert.Equal(1, indexEntriesObjectList[0].Value);
                Assert.Equal(21, indexEntriesObjectList[1].Value);
                Assert.Equal(37, indexEntriesObjectList[2].Value);
                
                Assert.Null(queryResultsObjectList[0].Name);
                Assert.Null(queryResultsObjectList[1].Name);
                Assert.Null(queryResultsObjectList[2].Name);
                Assert.Equal(1, queryResultsObjectList[0].Value);
                Assert.Equal(21, queryResultsObjectList[1].Value);
                Assert.Equal(37, queryResultsObjectList[2].Value);
                
                Assert.Null(mapResultsObjectList[0].Name);
                Assert.Null(mapResultsObjectList[1].Name);
                Assert.Null(mapResultsObjectList[2].Name);
                Assert.Equal(1, mapResultsObjectList[0].Value);
                Assert.Equal(21, mapResultsObjectList[1].Value);
                Assert.Equal(37, mapResultsObjectList[2].Value);
                
                Assert.Empty(reduceResultsObjectList);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    private void CountersLinqMapReduce(Options options) => TestMapReduceIndexOnCounters(options,
        new Payload()
        {
            IndexDefinition = new IndexDefinition()
            {
                Name = "CoolLinqMapReduceIndex",
                Maps = new HashSet<string>
                {
                    "from counter in counters.Dtos.Likes select new { Name = counter.Name, Value = counter.Value }"
                },
                Reduce = "from result in results group result by new { result.Name } into g select new { Name = g.Key.Name, Value = g.Sum(x => x.Value) }"
            },
            Query = new QueryClass()
            {
                Query = "from index 'CoolLinqMapReduceIndex' select Value"
            }
        });
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    private void CountersJsMapReduce(Options options) => TestMapReduceIndexOnCounters(options,
        new Payload()
        {
            IndexDefinition = new IndexDefinition()
            {
                Name = "CoolJsMapReduceIndex",
                Maps = new HashSet<string>
                {
                    "counters.map('Dtos', 'Likes', function (counter) { return { Name: counter.Name, Value: counter.Value } })"
                },
                Reduce = "groupBy(x => ({ Name: x.Name })).aggregate(g => { return { Name: g.key.Name, Value: g.values.reduce((count, val) => val.Value + count, 0) } })"
            },
            Query = new QueryClass()
            {
                Query = "from index 'CoolJsMapReduceIndex' select Value"
            }
        });
    
    private void TestMapReduceIndexOnCounters(Options options, Payload payload, bool isJs = false)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Name = "Name1", Age = 21 };
                var dto2 = new Dto() { Name = "Name2", Age = 37 };
                var dto3 = new Dto() { Name = "Name3", Age = 55 };
                
                session.Store(dto1);
                session.Store(dto2);
                session.Store(dto3);
                
                var c1 = session.CountersFor(dto1.Id);
                var c2 = session.CountersFor(dto2.Id);
                var c3 = session.CountersFor(dto3.Id);
                
                c1.Increment("Likes");
                c2.Increment("Likes", 21);
                c3.Increment("Likes", 37);
                
                session.SaveChanges();
            }
            
            using (var commands = store.Commands())
            {
                var cmd = new PutTestIndexCommand(payload);
                commands.Execute(cmd);
                
                var res = cmd.Result as BlittableJsonReaderObject;
                
                Assert.NotNull(res);
                
                res.TryGet(nameof(TestIndexResult.IndexEntries), out BlittableJsonReaderArray indexEntries);
                res.TryGet(nameof(TestIndexResult.QueryResults), out BlittableJsonReaderArray queryResults);
                res.TryGet(nameof(TestIndexResult.MapResults), out BlittableJsonReaderArray mapResults);
                res.TryGet(nameof(TestIndexResult.ReduceResults), out BlittableJsonReaderArray reduceResults);
                
                var indexEntriesObjectList = JsonConvert.DeserializeObject<List<DtoForCounters>>(indexEntries.ToString());
                var queryResultsObjectList = JsonConvert.DeserializeObject<List<DtoForCounters>>(queryResults.ToString());
                var mapResultsObjectList = JsonConvert.DeserializeObject<List<DtoForCounters>>(mapResults.ToString());
                var reduceResultsObjectList = JsonConvert.DeserializeObject<List<DtoForCounters>>(reduceResults.ToString());
                
                Assert.Equal("likes", indexEntriesObjectList[0].Name);
                Assert.Equal(59, indexEntriesObjectList[0].Value);

                Assert.Null(queryResultsObjectList[0].Name);
                Assert.Equal(59, queryResultsObjectList[0].Value);
                
                Assert.Equal("Likes", mapResultsObjectList[0].Name);
                Assert.Equal("Likes", mapResultsObjectList[1].Name);
                Assert.Equal("Likes", mapResultsObjectList[2].Name);
                Assert.Equal(1, mapResultsObjectList[0].Value);
                Assert.Equal(21, mapResultsObjectList[1].Value);
                Assert.Equal(37, mapResultsObjectList[2].Value);
                
                Assert.Equal("Likes", reduceResultsObjectList[0].Name);
                Assert.Equal(59, reduceResultsObjectList[0].Value);
            }
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void CheckIfOutputReduceToCollectionDoesNotStoreDocumentsForTestIndex()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { Name = "Name1", Age = 21 };
                var dto2 = new Dto() { Name = "Name2", Age = 37 };
                var dto3 = new Dto() { Name = "Name1", Age = 55 };

                session.Store(dto1);
                session.Store(dto2);
                session.Store(dto3);

                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var payload = new Payload()
                {
                    IndexDefinition = new IndexDefinition()
                    {
                        Name = "LinqMapReduceIndexWithOutputToCollection",
                        Maps = new HashSet<string> { "from dto in docs.Dtos select new { Name = dto.Name, Age = dto.Age, Count = 1 }" },
                        Reduce =
                            "from result in results group result by new { result.Name } into g select new { Name = g.Key.Name, Count = g.Sum(x => x.Count), Age = g.Sum(x => x.Age) }",
                        OutputReduceToCollection = "OutputCollection"
                    },
                    Query = new QueryClass() { Query = "from index 'LinqMapReduceIndexWithOutputToCollection' select Count" }
                };

                var cmd = new PutTestIndexCommand(payload);
                commands.Execute(cmd);
            }

            using (var session = store.OpenSession())
            {
                var query = session.Advanced.RawQuery<object>("from 'OutputCollection'");

                var res = query.ToList();
                
                Assert.Empty(res);
            }
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public double Age { get; set; }
        
        public double Count { get; set; }
    }
    
    private class DtoForTS
    {
        public string Tag { get; set; }
        public double FirstValue { get; set; }
    }
    
    private class DtoForCounters
    {
        public string Name { get; set; }
        public double Value { get; set; }
    }

    private class Cat
    {
        public string Id { get; set; }
        
        public string Name { get; set; }
        public double Age { get; set; }
    }
    
    private class Payload
    {
        public IndexDefinition IndexDefinition { get; set; }
#nullable enable
        public QueryClass? Query { get; set; }
#nullable disable
    }
    
    private class QueryClass
    {
        public string Query { get; set; }
    }
}

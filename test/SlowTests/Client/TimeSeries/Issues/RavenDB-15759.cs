using System;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15759 : ReplicationTestBase
    {
        public RavenDB_15759(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task TimeSeriesCollectionQueryFromStudioWithNamedValues_ShouldNotThrowOnDocumentsWithoutTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    // create 2 users, one with time-series and one without
                    
                    session.Store(new User(), "users/ayende");
                    session.Store(new User(), "users/aviv");

                    session.TimeSeriesFor("users/aviv", "HeartRate").Append(DateTime.Now, 1);

                    session.SaveChanges();
                }

                // configure named values 

                var nameConfig = new ConfigureTimeSeriesValueNamesOperation(new ConfigureTimeSeriesValueNamesOperation.Parameters
                {
                    Collection = "Users",
                    TimeSeries = "HeartRate",
                    ValueNames = new[] { "HeartRate" },
                    Update = true
                });

                await store.Maintenance.SendAsync(nameConfig);

                // create index query, query command and http request
 
                var indexQuery = new IndexQuery
                {
                    Query = @"from Users select timeseries(from HeartRate)"
                };

                var serverNode = new ServerNode
                {
                    Database = store.Database,
                    Url = store.Urls.First()
                };

                using (var session = store.OpenSession())
                {
                    var queryCommand = new QueryCommand((InMemoryDocumentSessionOperations)session, indexQuery);

                    using (var re = store.GetRequestExecutor())
                    using (re.ContextPool.AllocateOperationContext(out var ctx))
                    {
                        var request = queryCommand.CreateRequest(ctx, serverNode, out var url);
                        request.RequestUri = new UriBuilder(url).Uri;

                        // add studio header so that 'TimeSeriesNamedValues' will be part of the results
                        request.Headers.Add(Constants.Headers.StudioVersion, "foobar");

                        var response = await re.HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, CancellationToken.None);
                        await queryCommand.ProcessResponse(ctx, re.Cache, response, url);

                        // assert

                        var results = queryCommand.Result.Results;
                        Assert.Equal(2, results.Length);

                        var doc = results[0] as BlittableJsonReaderObject;
                        Assert.NotNull(doc);
                        Assert.True(doc.TryGet("Count", out long count));
                        Assert.Equal(0, count);

                        Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
                        Assert.True(metadata.TryGet(Constants.Documents.Metadata.Id, out string id));
                        Assert.Equal("users/ayende", id);
                        
                        Assert.True(metadata.TryGet(Constants.Documents.Metadata.TimeSeriesNamedValues, out BlittableJsonReaderArray namedValues));
                        Assert.Equal(1, namedValues.Length);

                        doc = results[1] as BlittableJsonReaderObject;
                        Assert.NotNull(doc);
                        Assert.True(doc.TryGet("Count", out count));
                        Assert.Equal(1, count);

                        Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out metadata));
                        Assert.True(metadata.TryGet(Constants.Documents.Metadata.Id, out id));
                        Assert.Equal("users/aviv", id);

                        Assert.True(metadata.TryGet(Constants.Documents.Metadata.TimeSeriesNamedValues, out namedValues));
                        Assert.Equal(1, namedValues.Length);

                    }
                }
            }
        }
    }
}

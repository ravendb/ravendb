using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Database.Config;
using Raven.Database.Indexing;
using Raven.Database.Prefetching;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3539 : RavenTestBase
    {
        public class Person
        {
            public string Name { get; set; }
            public string Email { get; set; }

        }

        public class Simple : AbstractIndexCreationTask<Person>
        {
            public Simple()
            {
                this.Map = results => from result in results
                    select new
                    {
                        Name = result.Name,
                        Email = result.Email
                    };
            }
        }


        [Fact]
        public void get_debug_info_ForSpecifiedDatabase()
        {
            using (var documentStore = NewDocumentStore())
            {

                documentStore.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                {
                    Id = "Dba1",
                    Settings =
                    {
                        {"Raven/DataDir", "Dba1"}
                    }
                });

                new Simple().Execute(documentStore.DatabaseCommands.ForDatabase("Dba1"), documentStore.Conventions);
                
                using (var session = documentStore.OpenSession("Dba1"))
                {
                    var entity1 = new Person {Name = "Rob", Email = "person1@gmail.com"};
                    var entity2 = new Person { Name = "Haim", Email = "person2@gmail.com" };
                    var entity3 = new Person { Name = "Rob", Email = "person3@gmail.com" };

                    session.Store(entity1);
                    session.Store(entity2);
                    session.Store(entity3);

                    var persons = session.Query<Person>().Where(x => x.Name == "Rob").ToList();
                }

                var dbWorkContext = documentStore.ServerIfEmbedded.Options.DatabaseLandlord.GetResourceInternal("Dba1").Result.WorkContext;

                var dbName = dbWorkContext.DatabaseName;

                var independentBatchSizeAutoTuner = new IndexBatchSizeAutoTuner(dbWorkContext);

                var prefetchingBehavior = new PrefetchingBehavior(PrefetchingUser.Indexer, dbWorkContext, independentBatchSizeAutoTuner, string.Empty);
                prefetchingBehavior.HandleLowMemory();

                independentBatchSizeAutoTuner.AutoThrottleBatchSize(100, 1024, TimeSpan.MinValue);
                independentBatchSizeAutoTuner.HandleLowMemory();
                

                MemoryStatistics.RunLowMemoryHandlers("System detected low memory");

                var url = $"http://localhost:8079/databases/{dbName}/debug/auto-tuning-info";
                var requestWithDbName = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
                    documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
                var results = requestWithDbName.ReadResponseJson().JsonDeserialization<AutoTunerInfo>();
                var reason = results.Reason;
                var reasonForLowMemoryCall = results.LowMemoryCallsRecords.First().Reason;
                var lowMemoryRecords = results.LowMemoryCallsRecords.First().Operations;
                Assert.Equal("System detected low memory", reasonForLowMemoryCall);

                var urlAdmin = $"http://localhost:8079/databases/{dbName}/admin/debug/auto-tuning-info";
                var requestWithDbNameAdmin = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
                documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
                var resultsAdmin = requestWithDbNameAdmin.ReadResponseJson().JsonDeserialization<AutoTunerInfo>();
                var reasonAdmin = results.Reason;
                var reasonForLowMemoryCallAdmin = resultsAdmin.LowMemoryCallsRecords.First().Reason;
                var lowMemoryRecordsAdmin = resultsAdmin.LowMemoryCallsRecords.First().Operations;
                var cpuUsageRecordsAdmin = resultsAdmin.CpuUsageCallsRecords;
                Assert.Equal("System detected low memory", reasonForLowMemoryCallAdmin);
            }
        }

        [Fact]
        public void get_debug_info_ForAdmin()
        {
            using (var documentStore = NewDocumentStore())
            {

                var workContext = documentStore.SystemDatabase.WorkContext;

                var prefetchingBehavior = new PrefetchingBehavior(PrefetchingUser.Indexer, workContext, new IndexBatchSizeAutoTuner(workContext), string.Empty);


                MemoryStatistics.RunLowMemoryHandlers("System detected low memory");

                var prefetchingBehavior2 = new PrefetchingBehavior(PrefetchingUser.Indexer, workContext, new IndependentBatchSizeAutoTuner(workContext, PrefetchingUser.Indexer), string.Empty);

                var prefetchingBehavior3 = new PrefetchingBehavior(PrefetchingUser.Indexer, workContext, new ReduceBatchSizeAutoTuner(workContext), string.Empty);

                var indexBatchAutoTuner = new IndependentBatchSizeAutoTuner(workContext, prefetchingBehavior.PrefetchingUser);
                var independentBatchSizeAutoTuner = new IndependentBatchSizeAutoTuner(workContext, prefetchingBehavior2.PrefetchingUser);
                var reduceBatchSizeAutoTuner = new IndependentBatchSizeAutoTuner(workContext, prefetchingBehavior3.PrefetchingUser);

                independentBatchSizeAutoTuner.AutoThrottleBatchSize(100, 1024, TimeSpan.MinValue);

                reduceBatchSizeAutoTuner.AutoThrottleBatchSize(500, 1024, TimeSpan.MinValue);
                indexBatchAutoTuner.HandleLowMemory();


                reduceBatchSizeAutoTuner.AutoThrottleBatchSize(100, 1024, TimeSpan.MinValue);
                prefetchingBehavior3.HandleLowMemory();

                indexBatchAutoTuner.AutoThrottleBatchSize(100, 1024, TimeSpan.MinValue);
                prefetchingBehavior2.HandleLowMemory();

                prefetchingBehavior.OutOfMemoryExceptionHappened();
                prefetchingBehavior.HandleLowMemory();
                MemoryStatistics.RunLowMemoryHandlers("System detected low memory");

                var url = "http://localhost:8079/admin/debug/auto-tuning-info";
                var requestWithDbName = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
                    documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
                var results = requestWithDbName.ReadResponseJson().JsonDeserialization<AutoTunerInfo>();

                var reason = results.Reason;
                var reasonForLowMemoryCall = results.LowMemoryCallsRecords.First().Reason;
                var lowMemoryRecords = results.LowMemoryCallsRecords.First().Operations;
                //System notification, low memory
                Assert.Equal("System detected low memory", reasonForLowMemoryCall);

            }
        }
        [Fact]
        public void get_debug_info_ForSpecifiedDatabase_IndependentBatchSizeAutoTuner()
        {
            using (var documentStore = NewDocumentStore())
            {

                documentStore.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                {
                    Id = "Dba1",
                    Settings =
                    {
                        {"Raven/DataDir", "Dba1"}
                    }
                });
                var dbWorkContext = documentStore.ServerIfEmbedded.Options.DatabaseLandlord.GetResourceInternal("Dba1").Result.WorkContext;

                var dbName = dbWorkContext.DatabaseName;

                var independentBatchSizeAutoTuner = new IndependentBatchSizeAutoTuner(dbWorkContext, PrefetchingUser.Indexer);
                var prefetchingBehavior = new PrefetchingBehavior(PrefetchingUser.Indexer, dbWorkContext, independentBatchSizeAutoTuner, string.Empty);
                
                independentBatchSizeAutoTuner.NumberOfItemsToProcessInSingleBatch = 200;

                independentBatchSizeAutoTuner.AutoThrottleBatchSize(100, 1024, TimeSpan.MinValue);
                
                independentBatchSizeAutoTuner.HandleLowMemory();
                MemoryStatistics.RunLowMemoryHandlers("System detected low memory");

                prefetchingBehavior.OutOfMemoryExceptionHappened();
                prefetchingBehavior.HandleLowMemory();
            
                var url = $"http://localhost:8079/databases/{dbName}/debug/auto-tuning-info";
                var requestWithDbName = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
                    documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
                var results = requestWithDbName.ReadResponseJson().JsonDeserialization<AutoTunerInfo>();

                var reason = results.Reason;
                var reasonForLowMemoryCall = results.LowMemoryCallsRecords.First().Reason;
                var lowMemoryRecords = results.LowMemoryCallsRecords.First().Operations;

                Assert.Equal("System detected low memory", reasonForLowMemoryCall);

                var urlAdmin = $"http://localhost:8079/databases/{dbName}/admin/debug/auto-tuning-info";
                var requestWithDbNameAdmin = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
                documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
                var resultsAdmin = requestWithDbNameAdmin.ReadResponseJson().JsonDeserialization<AutoTunerInfo>();
                var reasonAdmin = results.Reason;
                var reasonForLowMemoryCallAdmin = resultsAdmin.LowMemoryCallsRecords.First().Reason;
                var lowMemoryRecordsAdmin = resultsAdmin.LowMemoryCallsRecords.First().Operations;
                var cpuUsageRecordsAdmin = resultsAdmin.CpuUsageCallsRecords;
                Assert.Equal("System detected low memory", reasonForLowMemoryCallAdmin);
            }
        }
        [Fact]
        public void get_debug_info_ForSpecifiedDatabase_IndexBatchSizeAutoTuner()
        {
            using (var documentStore = NewDocumentStore())
            {

                documentStore.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                {
                    Id = "Dba1",
                    Settings =
                    {
                        {"Raven/DataDir", "Dba1"}
                    }
                });
                var dbWorkContext = documentStore.ServerIfEmbedded.Options.DatabaseLandlord.GetResourceInternal("Dba1").Result.WorkContext;

                var dbName = dbWorkContext.DatabaseName;

                var indexBatchAutoTuner = new IndexBatchSizeAutoTuner(dbWorkContext);
                var prefetchingBehavior = new PrefetchingBehavior(PrefetchingUser.Indexer, dbWorkContext, indexBatchAutoTuner, string.Empty);

                indexBatchAutoTuner.AutoThrottleBatchSize(100, 1024, TimeSpan.MinValue);

                indexBatchAutoTuner.HandleLowMemory();
                MemoryStatistics.RunLowMemoryHandlers("System detected low memory");

                prefetchingBehavior.OutOfMemoryExceptionHappened();
                prefetchingBehavior.HandleLowMemory();

                var url = $"http://localhost:8079/databases/{dbName}/debug/auto-tuning-info";
                var requestWithDbName = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
                    documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
                var results = requestWithDbName.ReadResponseJson().JsonDeserialization<AutoTunerInfo>();
                var reason = results.Reason;
                var reasonForLowMemoryCall = results.LowMemoryCallsRecords.First().Reason;
                var lowMemoryRecords = results.LowMemoryCallsRecords.First().Operations;
                Assert.Equal("System detected low memory", reasonForLowMemoryCall);

                
                var urlAdmin = $"http://localhost:8079/databases/{dbName}/admin/debug/auto-tuning-info";
                var requestWithDbNameAdmin = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
                documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
                var resultsAdmin = requestWithDbNameAdmin.ReadResponseJson().JsonDeserialization<AutoTunerInfo>();
                var reasonAdmin = results.Reason;
                var reasonForLowMemoryCallAdmin = resultsAdmin.LowMemoryCallsRecords.First().Reason;
                var lowMemoryRecordsAdmin = resultsAdmin.LowMemoryCallsRecords.First().Operations;
                var cpuUsageRecordsAdmin = resultsAdmin.CpuUsageCallsRecords;
                Assert.Equal("System detected low memory", reasonForLowMemoryCallAdmin);
            }
        }
        [Fact]
        public void get_debug_info_ForSpecifiedDatabaseReducedBatchSizeAutoTuner()
        {
            using (var documentStore = NewDocumentStore())
            {

                documentStore.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                {
                    Id = "Dba1",
                    Settings =
                    {
                        {"Raven/DataDir", "Dba1"}
                    }
                });
                var dbWorkContext = documentStore.ServerIfEmbedded.Options.DatabaseLandlord.GetResourceInternal("Dba1").Result.WorkContext;

                var dbName = dbWorkContext.DatabaseName;

                var reduceBatchSizeAutoTuner = new ReduceBatchSizeAutoTuner(dbWorkContext);
                var prefetchingBehavior = new PrefetchingBehavior(PrefetchingUser.Indexer, dbWorkContext, reduceBatchSizeAutoTuner, string.Empty);

                MemoryStatistics.SimulateLowMemoryNotification();

                reduceBatchSizeAutoTuner.AutoThrottleBatchSize(100, 1024, TimeSpan.MinValue);
                reduceBatchSizeAutoTuner.HandleLowMemory();

                prefetchingBehavior.HandleLowMemory();
                MemoryStatistics.RunLowMemoryHandlers("System detected low memory");

                prefetchingBehavior.OutOfMemoryExceptionHappened();
                prefetchingBehavior.HandleLowMemory();

                var url = $"http://localhost:8079/databases/{dbName}/debug/auto-tuning-info";
                var requestWithDbName = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get,
                    documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
                var results = requestWithDbName.ReadResponseJson().JsonDeserialization<AutoTunerInfo>();
                var reason = results.Reason;
                var reasonForLowMemoryCall = results.LowMemoryCallsRecords.First().Reason;
                var lowMemoryRecords = results.LowMemoryCallsRecords.First().Operations;
                Assert.Equal("System detected low memory", reasonForLowMemoryCall);


                var urlAdmin = $"http://localhost:8079/databases/{dbName}/admin/debug/auto-tuning-info";
                var requestWithDbNameAdmin = documentStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, urlAdmin, HttpMethods.Get,
                documentStore.DatabaseCommands.PrimaryCredentials, documentStore.Conventions));
                var resultsAdmin = requestWithDbNameAdmin.ReadResponseJson().JsonDeserialization<AutoTunerInfo>();
                var reasonAdmin = results.Reason;
                var reasonForLowMemoryCallAdmin = resultsAdmin.LowMemoryCallsRecords.First().Reason;
                var lowMemoryRecordsAdmin = resultsAdmin.LowMemoryCallsRecords.First().Operations;
                var cpuUsageRecordsAdmin = resultsAdmin.CpuUsageCallsRecords;
                Assert.Equal("System detected low memory", reasonForLowMemoryCallAdmin);
            }
        }
    }
}
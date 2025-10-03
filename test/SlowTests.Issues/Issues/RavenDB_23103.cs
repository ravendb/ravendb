using System;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_23103 : RavenTestBase
    {
        public RavenDB_23103(ITestOutputHelper output) : base(output)
        {
        }

        private const int NumberOfCompanies = 1024;
        /****************STATIC************************/
        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_Simple_Static()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseRecord = record =>
                           record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);
                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery
                {
                    Query = $"from index '{index.IndexName}' as c update {{ c.Name = 'Name2' }}"
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        IndexPatchOptions = new IndexPatchOptions(TimeSpan.FromSeconds(30))
                        {
                            ThrowOnTimeoutInWaitForIndexes = true
                        }
                    }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.Equal(NumberOfCompanies, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_StaticIndex_WithTimeout_NoSpecificIndexes()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
            }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = $"from index '{index.IndexName}' as c update {{ c.Name = 'Name2' }}" };

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions
                {
                    WaitForIndexesTimeout = TimeSpan.FromMicroseconds(1),
                    ThrowOnTimeoutInWaitForIndexes = true
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        IndexPatchOptions = waitForIndexingAfterPatchOptions
                    }));

                Assert.ThrowsAny<Exception>(() => operation.WaitForCompletion(TimeSpan.FromSeconds(30)));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_StaticIndex_WithTimeout_WithSpecificIndexes()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
            }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                

                var iq = new IndexQuery { Query = $"from index '{index.IndexName}' as c update {{ c.Name = 'Name3' }}" };

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions
                {
                    WaitForIndexesTimeout = TimeSpan.FromMicroseconds(1),
                    WaitForSpecificIndexes = new[] { index.IndexName },
                    ThrowOnTimeoutInWaitForIndexes = true
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions { IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                Assert.ThrowsAny<Exception>(() => operation.WaitForCompletion(TimeSpan.FromSeconds(30)));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_StaticIndex_WithoutTimeout_WithSpecificIndexes()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = $"from index '{index.IndexName}' as c update {{ c.Name = 'Name2' }}" };

                var waitForIndexBatchOptions = new IndexPatchOptions
                {
                    WaitForSpecificIndexes = new[] { index.IndexName },
                    ThrowOnTimeoutInWaitForIndexes = true
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions { IndexPatchOptions = waitForIndexBatchOptions }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.Equal(NumberOfCompanies, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_StaticIndex_WithoutTimeout_NoSpecificIndexes()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
            }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = $"from index '{index.IndexName}' as c update {{ c.Name = 'Name2' }}" };

                var waitForIndexBatchOptions = new IndexPatchOptions
                {
                    ThrowOnTimeoutInWaitForIndexes = true
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions { IndexPatchOptions = waitForIndexBatchOptions }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.Equal(NumberOfCompanies, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_StaticIndex_WithTimeoutShouldntThrow()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
            }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = $"from index '{index.IndexName}' as c update {{ c.Name = 'Name2' }}" };

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions
                {
                    WaitForIndexesTimeout = TimeSpan.FromMicroseconds(1)
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions { IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.NotEqual(NumberOfCompanies, count);
                }
            }
        }


        /****************COLLECTIONS************************/
        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_Simple_CollectionQuery()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        session.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = $"from companies as c update {{ c.Name = 'Name2' }}" };

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions
                {
                    WaitForIndexesTimeout = TimeSpan.FromSeconds(30)
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions { IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.Equal(NumberOfCompanies, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_CollectionQuery_WithTimeout()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
            }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = $"from companies as c update {{ c.Name = 'Name2' }}" };

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions
                {
                    WaitForIndexesTimeout = TimeSpan.FromMicroseconds(1),
                    ThrowOnTimeoutInWaitForIndexes = true
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions { IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                Assert.ThrowsAny<Exception>(() => operation.WaitForCompletion(TimeSpan.FromSeconds(30)));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_CollectionQuery_WithoutTimeout()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
            }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = $"from companies as c update {{ c.Name = 'Name2' }}" };

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions
                {
                    ThrowOnTimeoutInWaitForIndexes = true
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions { IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.Equal(NumberOfCompanies, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_CollectionQuery_WithTimeoutShouldntThrow()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = $"from companies as c update {{ c.Name = 'Name2' }}" };

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions
                {
                    WaitForIndexesTimeout = TimeSpan.FromMicroseconds(1)
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions { IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.NotEqual(NumberOfCompanies, count);
                }
            }
        }


        /****************AllDocs************************/
        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_Simple_AllDocsQuery()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        session.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = "from @all_docs as c update { c.Name = 'Name2' }" };

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions
                {
                    WaitForIndexesTimeout = TimeSpan.FromSeconds(30),
                    ThrowOnTimeoutInWaitForIndexes = true
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions { IndexPatchOptions = waitForIndexingAfterPatchOptions }));


                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.Equal(NumberOfCompanies, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_AllDocsQuery_WithTimeout()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
            }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = $"from @all_docs as c update {{ c.Name = 'Name2' }}" };

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions
                {
                    WaitForIndexesTimeout = TimeSpan.FromMicroseconds(1),
                    ThrowOnTimeoutInWaitForIndexes = true
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions { IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                Assert.ThrowsAny<Exception>(() => operation.WaitForCompletion(TimeSpan.FromSeconds(30)));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_AllDocsQuery_WithoutTimeout()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
            }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = $"from @all_docs as c update {{ c.Name = 'Name2' }}" };

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions()
                {
                    ThrowOnTimeoutInWaitForIndexes = true
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions { IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.Equal(NumberOfCompanies, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_AllDocsQuery_WithTimeoutShouldntThrow()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
            }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = $"from @all_docs as c update {{ c.Name = 'Name2' }}" };

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions()
                {
                    WaitForIndexesTimeout = TimeSpan.FromMicroseconds(1)
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions { IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.NotEqual(NumberOfCompanies, count);
                }
            }
        }


        /****************DYNAMIC************************/
        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_Simple_Dynamic()
        {


            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseRecord = record =>
                           record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Company>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "Name1").ToList();
                    Assert.Equal(NumberOfCompanies, results.Count);
                }

                var iq = new IndexQuery { Query = $"from companies as c where c.Name != null update {{ c.Name = 'Name2' }}" };
                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        IndexPatchOptions = new IndexPatchOptions()
                        {
                            WaitForIndexesTimeout = TimeSpan.FromSeconds(30),
                            ThrowOnTimeoutInWaitForIndexes = true
                        }
                    }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company>().Count(x => x.Name == "Name2");
                    Assert.Equal(NumberOfCompanies, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_DynamicQuery_WithTimeout()
        {
            using (var store = GetDocumentStore(new Options 
                   {
                       ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = $"from companies as c where c.Name != null update {{ c.Name = 'Name2' }}" };

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions
                {
                    WaitForIndexesTimeout = TimeSpan.FromMicroseconds(1), ThrowOnTimeoutInWaitForIndexes = true
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions { IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                Assert.ThrowsAny<Exception>(() => operation.WaitForCompletion(TimeSpan.FromSeconds(30)));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_DynamicQuery_WithoutTimeout()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = $"from companies as c where c != null update {{ c.Name = 'Name2' }}" };

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions
                {
                    ThrowOnTimeoutInWaitForIndexes = true
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions { IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.Equal(NumberOfCompanies, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void WaitForIndexesAfterPatch_DynamicQuery_WithTimeoutShouldntThrow()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
            }))
            {
                var indexDef = new Companies_ByName();
                indexDef.Execute(store);
                string indexName;

                using (var session = store.OpenSession())
                {
                    session.Query<Company>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == "___")
                        .ToList();

                    indexName = stats.IndexName;
                }

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                var database = Databases.GetDocumentDatabaseInstanceFor(store).Result;
                database.IndexStore.GetIndex(indexName).ForTestingPurposesOnly().CallDuringFinallyOfExecuteIndexing(() => Thread.Sleep(1));
                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = $"from companies as c where c != null update {{ c.Name = 'Name2' }}" };

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions()
                {
                    WaitForIndexesTimeout = TimeSpan.FromMilliseconds(1)
                };

                var operation = store.Operations.Send(new PatchByQueryOperation(iq,
                    new QueryOperationOptions { IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                     Assert.NotEqual(NumberOfCompanies, count);
                }
            }
        }


        /****************DELETE************************/
        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void DeleteByQuery_With_WaitForIndexes_Static()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
            }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery
                {
                    Query = $"from index '{index.IndexName}'"
                };

                var operation = store.Operations.Send(new DeleteByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        AllowStale = false,
                        IndexPatchOptions = new IndexPatchOptions()
                        {
                            WaitForIndexesTimeout = TimeSpan.FromSeconds(30)
                        }
                    }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count();
                    Assert.Equal(0, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void DeleteByQuery_With_WaitForIndexes_AllDocs()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseRecord = record =>
                           record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery
                {
                    Query = $"from @all_docs"
                };

                var operation = store.Operations.Send(new DeleteByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        AllowStale = false,
                        IndexPatchOptions = new IndexPatchOptions()
                        {
                            WaitForIndexesTimeout = TimeSpan.FromSeconds(30)
                        }
                    }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count();
                    Assert.Equal(0, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void DeleteByQuery_With_WaitForIndexes_Static_WithTimeout()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
            }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery
                {
                    Query = $"from index '{index.IndexName}'"
                };

                var operation = store.Operations.Send(new DeleteByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        IndexPatchOptions = new IndexPatchOptions()
                        {
                            WaitForIndexesTimeout = TimeSpan.FromMicroseconds(1),
                            ThrowOnTimeoutInWaitForIndexes = true
                        }
                    }));

                Assert.ThrowsAny<Exception>(() => operation.WaitForCompletion(TimeSpan.FromSeconds(30)));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void DeleteByQuery_With_WaitForIndexes_Static_WithoutTimeout()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery
                {
                    Query = $"from index '{index.IndexName}'"
                };

                var operation = store.Operations.Send(new DeleteByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        IndexPatchOptions = new IndexPatchOptions()
                        {
                            ThrowOnTimeoutInWaitForIndexes = true
                        }
                    }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.NotEqual(NumberOfCompanies, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void DeleteByQuery_With_WaitForIndexes_Static_WithTimeout_WithTimeoutShouldntThrow()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery
                {
                    Query = $"from index '{index.IndexName}'"
                };

                var operation = store.Operations.Send(new DeleteByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        IndexPatchOptions = new IndexPatchOptions()
                        {
                            WaitForIndexesTimeout = TimeSpan.FromMicroseconds(1)
                        }
                    }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.NotEqual(NumberOfCompanies, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void DeleteByQuery_With_WaitForIndexes_Collection()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseRecord = record =>
                           record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }

                    bulk.Store(new Company() { Name = "Name3" }, "Raven/1");
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery { Query = $"from Companies where startsWith(id(), 'Companies/')" };

                var operation = store.Operations.Send(new DeleteByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        IndexPatchOptions = new IndexPatchOptions()
                        {
                            WaitForIndexesTimeout = TimeSpan.FromSeconds(30)
                        }
                    }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count();
                    Assert.Equal(1, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void DeleteByQuery_With_WaitForIndexes_Collection_WithTimeout()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseRecord = record =>
                           record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                    bulk.Store(new Company() { Name = "Name3" }, "Raven/1");
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery
                {
                    Query = $"from Companies where startsWith(id(), 'Companies/')"
                };

                var operation = store.Operations.Send(new DeleteByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        IndexPatchOptions = new IndexPatchOptions()
                        {
                            WaitForIndexesTimeout = TimeSpan.FromMicroseconds(1),
                            ThrowOnTimeoutInWaitForIndexes = true
                        }
                    }));

                Assert.ThrowsAny<Exception>(() => operation.WaitForCompletion(TimeSpan.FromSeconds(30)));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void DeleteByQuery_With_WaitForIndexes_Collection_WithoutTimeout()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                    bulk.Store(new Company() { Name = "Name3" }, "Raven/1");
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery
                {
                    Query = $"from Companies where startsWith(id(), 'Companies/')"
                };

                var operation = store.Operations.Send(new DeleteByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        IndexPatchOptions = new IndexPatchOptions()
                        {
                            ThrowOnTimeoutInWaitForIndexes = true
                        }
                    }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.NotEqual(NumberOfCompanies, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void DeleteByQuery_With_WaitForIndexes_Collection_WithTimeout_WithTimeoutShouldntThrow()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDocumentStore = x => x.Conventions.WaitForIndexesAfterSaveChangesTimeout = TimeSpan.FromMicroseconds(1),
                       ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                    bulk.Store(new Company() { Name = "Name3" }, "Raven/1");
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery
                {
                    Query = $"from Companies where startsWith(id(), 'Companies/')"
                };

                var operation = store.Operations.Send(new DeleteByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        IndexPatchOptions = new IndexPatchOptions()
                        {
                            WaitForIndexesTimeout = TimeSpan.FromMicroseconds(1)
                        }
                    }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.NotEqual(NumberOfCompanies, count);
                }
            }
        }


        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void DeleteByQuery_With_WaitForIndexes_Dynamic()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseRecord = record =>
                           record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies*10; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                    bulk.Store(new Company() { Name = "Name3" }, "Raven/1");
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery
                {
                    Query = $"from companies as c Where c.Name == 'Name1'"
                };

                var operation = store.Operations.Send(new DeleteByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        AllowStale = false,
                        IndexPatchOptions = new IndexPatchOptions()
                        {
                            WaitForIndexesTimeout = TimeSpan.FromSeconds(30)
                        }
                    }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count();
                    Assert.Equal(1, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void DeleteByQuery_With_WaitForIndexes_Dynamic_WithTimeout()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseRecord = record =>
                           record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies * 10; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                    bulk.Store(new Company() { Name = "Name3" }, "Raven/1");
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery
                {
                    Query = $"from companies as c Where c.Name == 'Name1'"
                };

                var operation = store.Operations.Send(new DeleteByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        IndexPatchOptions = new IndexPatchOptions()
                        {
                            WaitForIndexesTimeout = TimeSpan.FromMicroseconds(1),
                            ThrowOnTimeoutInWaitForIndexes = true
                        }
                    }));

                Assert.ThrowsAny<Exception>(() => operation.WaitForCompletion(TimeSpan.FromSeconds(30)));
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void DeleteByQuery_With_WaitForIndexes_Dynamic_WithoutTimeout()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies * 10; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                    bulk.Store(new Company() { Name = "Name3" }, "Raven/1");
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery
                {
                    Query = $"from companies as c Where c.Name == 'Name1'"
                };

                var operation = store.Operations.Send(new DeleteByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        IndexPatchOptions = new IndexPatchOptions()
                        {
                            ThrowOnTimeoutInWaitForIndexes = true
                        }
                    }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.NotEqual(NumberOfCompanies, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void DeleteByQuery_With_WaitForIndexes_Dynamic_WithTimeout_WithTimeoutShouldntThrow()
        {
            using (var store = GetDocumentStore(new Options
                   {
                       ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = 128.ToString()
                   }))
            {
                var index = new Companies_ByName();
                index.Execute(store);

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < NumberOfCompanies * 10; i++)
                    {
                        bulk.Store(new Company { Name = "Name1" }, "Companies/" + i);
                    }
                    bulk.Store(new Company() { Name = "Name3" }, "Raven/1");
                }

                Indexes.WaitForIndexing(store);

                var iq = new IndexQuery
                {
                    Query = $"from companies as c Where c.Name == 'Name1'"
                };

                var operation = store.Operations.Send(new DeleteByQueryOperation(iq,
                    new QueryOperationOptions
                    {
                        IndexPatchOptions = new IndexPatchOptions()
                        {
                            WaitForIndexesTimeout= TimeSpan.FromMicroseconds(1)
                        }
                    }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Company, Companies_ByName>().Count(x => x.Name == "Name2");
                    Assert.NotEqual(NumberOfCompanies, count);
                }
            }
        }


        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void PatchTwice_ShouldWaitOnFirstButNotOnSecond_OnMultiMapIndex()
        {
            using (var store = GetDocumentStore())
            {
                var index = new CompaniesAndCustomers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        session.Store(new Company { Name = "Name1" }, "Companies/" + i);
                        session.Store(new Employee { Name = "Name1" }, "Employee/" + i);
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions()
                {
                    WaitForIndexesTimeout = TimeSpan.FromSeconds(30),
                    ThrowOnTimeoutInWaitForIndexes = true
                };

                // ----------------------------
                // FIRST PATCH: Change Name1 -> Name2
                // ----------------------------
                var iq1 = new IndexQuery { Query = $"from index '{index.IndexName}' as x update {{ x.Name = 'Name2' ; x.FirstName = 'Name2'}}" };

                var operation1 = store.Operations.Send(new PatchByQueryOperation(iq1,
                    new QueryOperationOptions { AllowStale = true, RetrieveDetails = true, IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                operation1.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<CompaniesAndCustomers_ByName.Result, CompaniesAndCustomers_ByName>()
                        .Count(x => x.Name == "Name2");

                    Assert.Equal(2 * NumberOfCompanies, count);
                }

                // ----------------------------
                // SECOND PATCH: No actual change (Name2 -> Name2)
                // ----------------------------
                var iq2 = new IndexQuery { Query = $"from index '{index.IndexName}' as x update {{ x.Name = 'Name2'; x.FirstName = 'Name2' }}" };

                waitForIndexingAfterPatchOptions.WaitForIndexesTimeout = TimeSpan.Zero;

                var operation2 = store.Operations.Send(new PatchByQueryOperation(iq2,
                    new QueryOperationOptions { AllowStale = true, RetrieveDetails = true, IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                operation2.WaitForCompletion(TimeSpan.FromSeconds(30));
                //everything is ok if no exception was thrown
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void PatchTwiceShouldWaitOnFirstButNotOnSecondAndUpdateOnlyOneCollectionOnMultiMapIndex()
        {
            using (var store = GetDocumentStore())
            {
                var index = new CompaniesAndCustomers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < NumberOfCompanies; i++)
                    {
                        session.Store(new Company { Name = "Name1" }, "Companies/" + i);
                        session.Store(new Employee { Name = "Name1" }, "Employee/" + i);
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var waitForIndexingAfterPatchOptions = new IndexPatchOptions()
                {
                    WaitForIndexesTimeout = TimeSpan.FromMinutes(3),
                    ThrowOnTimeoutInWaitForIndexes = true
                };

                // ----------------------------
                // FIRST PATCH: Change Name1 -> Name2
                // ----------------------------
                var iq1 = new IndexQuery { Query = $"from index '{index.IndexName}' as x where x.Collection == 'Companies' update {{ x.Name = 'Name2' }}" };

                var operation1 = store.Operations.Send(new PatchByQueryOperation(iq1,
                    new QueryOperationOptions { AllowStale = true, RetrieveDetails = true, IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                operation1.WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var count = session.Query<CompaniesAndCustomers_ByName.Result, CompaniesAndCustomers_ByName>()
                        .Count(x => x.Name == "Name2");

                    Assert.Equal(NumberOfCompanies, count);
                }

                // ----------------------------
                // SECOND PATCH: No actual change (Name2 -> Name2)
                // ----------------------------
                var iq2 = new IndexQuery { Query = $"from index '{index.IndexName}' as x where x.Collection == 'Companies' update {{ x.Name = 'Name2' }}" };

                waitForIndexingAfterPatchOptions.WaitForIndexesTimeout = TimeSpan.Zero;

                var operation2 = store.Operations.Send(new PatchByQueryOperation(iq2,
                    new QueryOperationOptions { AllowStale = true, RetrieveDetails = true, IndexPatchOptions = waitForIndexingAfterPatchOptions }));

                operation2.WaitForCompletion(TimeSpan.FromSeconds(30));
                //everything is ok if no exception was thrown
            }
        }

        private class Companies_ByName : AbstractIndexCreationTask<Company>
        {
            public Companies_ByName()
            {
                Map = companies => from c in companies
                    select new { c.Name };
            }
        }

        private class CompaniesAndCustomers_ByName : AbstractMultiMapIndexCreationTask<CompaniesAndCustomers_ByName.Result>
        {
            public class Result
            {
                public string Name { get; set; }
                public string Collection { get; set; }
            }

            public CompaniesAndCustomers_ByName()
            {
                AddMap<Company>(companies =>
                    from c in companies
                    select new { Name = c.Name, Collection = "Companies" });

                AddMap<Employee>(employee =>
                    from c in employee
                    select new { Name = c.Name, Collection = "Employees" });
            }
        }

        private class Company
        {
            public decimal AccountsReceivable { get; set; }
            public string Id { get; set; }
            public string Name { get; set; }
        }
        private class Employee
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string LastName { get; set; }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_23909(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Auto(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var (configuration, _) = AddEmbeddingsGenerationTask(store);
            
            using (var session = store.OpenSession())
            {
                var aiTaskDone = Etl.WaitForEtlToComplete(store);
                
                session.Store(new Dto() { Name = "fruit" });
                session.SaveChanges();
                
                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
                var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
                Assert.True(queriesWorkerRegistered);
                Assert.True(indexingWorkerRegistered);
                
                var result = session.Query<Dto>()
                    .VectorSearch(x => 
                        x.WithText("Name")
                            .UsingTask(configuration.Identifier)
                            .TargetQuantization(VectorEmbeddingType.Int8), 
                        factory => factory.ByText("fruit"))
                    .ToList();
                
                Assert.Single(result);
            }
        }
    }
    
    [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task AlreadyQuantizedVectorShouldThrow(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var (configuration, _) = AddEmbeddingsGenerationTask(store, targetQuantization: VectorEmbeddingType.Int8);


            using (var session = store.OpenSession())
            {
                var aiTaskDone = Etl.WaitForEtlToComplete(store);
                
                session.Store(new Dto() { Name = "fruit" });
                session.SaveChanges();
                
                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
                var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
                Assert.True(queriesWorkerRegistered);
                Assert.True(indexingWorkerRegistered);
                
                try
                {
                    _ = session.Query<Dto>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .VectorSearch(x =>
                                x.WithText("Name")
                                    .UsingTask(configuration.Identifier)
                                    .TargetQuantization(VectorEmbeddingType.Binary),
                            factory => factory.ByText("fruit"))
                        .ToList();
                }
                catch (Exception ex) when (ex is RavenException { InnerException: InvalidOperationException innerEx }
                                           && innerEx.Message.Contains("is marked as errored"))
                {
                    // Expected exception
                }
                
                await Indexes.WaitForIndexingAsync(store, allowErrors: true);
                var indexErrors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: true);
                
                Assert.Single(indexErrors);
                Assert.Contains("Quantization cannot be performed on already quantized vector.", indexErrors[0].Errors.First().Error);
            }
        }
    }

    [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Static(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var (configuration, _) = AddEmbeddingsGenerationTask(store);

            
            using (var session = store.OpenSession())
            {
                var aiTaskDone = Etl.WaitForEtlToComplete(store);
                
                session.Store(new Dto() { Name = "fruit" });
                session.SaveChanges();
                
                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
                var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
                Assert.True(queriesWorkerRegistered);
                Assert.True(indexingWorkerRegistered);

                var index = new DummyIndex();
                await index.ExecuteAsync(store);
                await Indexes.WaitForIndexingAsync(store);
                
                var result = session.Query<DummyIndex.IndexEntry, DummyIndex>()
                    .VectorSearch(x =>
                            x.WithField(y => y.VectorFromTextEmbeddings),
                        factory => factory.ByText("fruit"))
                    .ProjectInto<Dto>()
                    .ToList();
                
                Assert.Single(result);
            }
        }
    }

    [RavenMultiplatformTheory(RavenTestCategory.Ai, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task StaticJs(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var (configuration, _) = AddEmbeddingsGenerationTask(store);

            
            using (var session = store.OpenSession())
            {
                var aiTaskDone = Etl.WaitForEtlToComplete(store);

                session.Store(new Dto() { Name = "fruit" });
                session.SaveChanges();

                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
                var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
                Assert.True(queriesWorkerRegistered);
                Assert.True(indexingWorkerRegistered);

                var index = new DummyJsIndex();
                await index.ExecuteAsync(store);
                await Indexes.WaitForIndexingAsync(store);
                
                var result = session.Query<DummyJsIndex.IndexEntry, DummyJsIndex>()
                    .VectorSearch(x =>
                            x.WithField(y => y.VectorFromTextEmbeddings),
                        factory => factory.ByText("fruit"))
                    .ProjectInto<Dto>()
                    .ToList();
                
                Assert.Single(result);
            }
        }
    }

    private class Dto
    {
        public string Name { get; set; }
    }

    private class DummyIndex : AbstractIndexCreationTask<Dto, DummyIndex.IndexEntry>
    {
        public class IndexEntry
        {
            public object VectorFromTextEmbeddings { get; set; }
        }
        
        public DummyIndex()
        {
            Map = dtos => from dto in dtos
                select new IndexEntry() { VectorFromTextEmbeddings = LoadVector("Name", "localaitask") };
            
            Vector("VectorFromTextEmbeddings", factory => factory.DestinationEmbedding(VectorEmbeddingType.Int8));
        }
    }
    
    private class DummyJsIndex : AbstractJavaScriptIndexCreationTask
    {
        public class IndexEntry
        {
            public object VectorFromTextEmbeddings { get; set; }
        }
        
        public DummyJsIndex()
        {
            Maps = new HashSet<string>()
            {
                @"map('Dtos', function (dto) {
                   return {
                       VectorFromTextEmbeddings: loadVector('Name', 'localaitask')
                   };
                })"
            };
            
            Fields = new();
            Fields.Add("VectorFromTextEmbeddings", new IndexFieldOptions()
            {
                Vector = new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Single, 
                    DestinationEmbeddingType = VectorEmbeddingType.Int8
                }
            });
        }
    }
}

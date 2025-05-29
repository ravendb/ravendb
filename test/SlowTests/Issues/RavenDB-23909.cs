using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Exceptions;
using SlowTests.Server.Documents.AI.Embeddings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23909(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void Auto(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var (configuration, _) = AddEmbeddingsGenerationTask(store);

            using (var session = store.OpenSession())
            {
                var aiTaskDone = Etl.WaitForEtlToComplete(store);
                
                session.Store(new Dto() { Name = "fruit" });
                session.SaveChanges();
                
                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
                
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
    
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AlreadyQuantizedVectorShouldThrow(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var (configuration, _) = AddEmbeddingsGenerationTask(store, targetQuantization: VectorEmbeddingType.Int8);

            using (var session = store.OpenSession())
            {
                var aiTaskDone = Etl.WaitForEtlToComplete(store);
                
                session.Store(new Dto() { Name = "fruit" });
                session.SaveChanges();
                
                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

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
                
                Indexes.WaitForIndexing(store, allowErrors: true);
                var indexErrors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: true);
                
                Assert.Single(indexErrors);
                Assert.Contains("Quantization cannot be performed on already quantized vector.", indexErrors[0].Errors.First().Error);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void Static(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            _ = AddEmbeddingsGenerationTask(store);

            using (var session = store.OpenSession())
            {
                var aiTaskDone = Etl.WaitForEtlToComplete(store);
                
                session.Store(new Dto() { Name = "fruit" });
                session.SaveChanges();
                
                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));

                var index = new DummyIndex();
                index.Execute(store);
                Indexes.WaitForIndexing(store);
                
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

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void StaticJs(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            _ = AddEmbeddingsGenerationTask(store);

            using (var session = store.OpenSession())
            {
                var aiTaskDone = Etl.WaitForEtlToComplete(store);

                session.Store(new Dto() { Name = "fruit" });
                session.SaveChanges();

                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
                
                var index = new DummyJsIndex();
                index.Execute(store);
                Indexes.WaitForIndexing(store);
                
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

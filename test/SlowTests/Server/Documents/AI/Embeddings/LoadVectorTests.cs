using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using System.Threading.Tasks;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class LoadVectorTests(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public async Task CanIndexSingleVectorGeneratedByEtl() => await CanIndexSingleVectorGeneratedByEtlBase<IndexByName>();

    [RavenMultiplatformFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public async Task CanIndexSingleVectorGeneratedByEtlJs() => await CanIndexSingleVectorGeneratedByEtlBase<IndexByNameJs>();

    private async Task CanIndexSingleVectorGeneratedByEtlBase<TIndex>() where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore();

        string id;
        using (var session = store.OpenSession())
        {
            var dto = new Dto { Name = "Joe" };
            session.Store(dto);
            session.SaveChanges();
            id = dto.Id;
        }

        var index = new TIndex();
        await index.ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(1, nullElements);

            var ex = Assert.Throws<InvalidQueryException>(()=> session.Query<Dto, TIndex>().VectorSearch(f => f.WithField(s => s.Vector), v => v.ByText("Joe")).ToList());
            Assert.Contains("Couldn't find Embeddings Generation task with 'localaitask' identifier", ex.Message);
        }

        await store.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));
        var etlStatus = Etl.WaitForEtlToComplete(store);
        var (config, connectionString) = AddEmbeddingsGenerationTask(store);
        Assert.True(await etlStatus.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);
        AssertEmbeddingsForPath(store, config, connectionString, "Name", ["Joe"], id);

        await store.Maintenance.SendAsync(new StartIndexOperation(index.IndexName));
        await Indexes.WaitForIndexingAsync(store);
        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<Dto, TIndex>()
                .VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("joe"), minimumSimilarity: 0.75f)
                .ToList();

            Assert.Single(byVector);
        }

        var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Joe"], id);
        etlStatus.Reset();
        using (var session = store.OpenSession())
        {
            var load = session.Load<Dto>(id);
            load.Name = "sdklfjklsadjkl;assdjaskll"; // lets change it to random string just not to have similar vector to previous one
            session.Store(load);
            session.SaveChanges();
        }

        Assert.True(await etlStatus.WaitAsync(DefaultEtlTimeout));
        await Indexes.WaitForIndexingAsync(store);
        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<Dto, TIndex>()
                .VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("Joe"), minimumSimilarity: 0.75f)
                .ToList();

            Assert.Empty(byVector);
        }

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["sdklfjklsadjkl;assdjaskll"], id);
    }

    [RavenMultiplatformFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public async Task CanIndexMultipleVectorGeneratedByEtl() => await CanIndexMultipleVectorGeneratedByEtlBase<IndexByNames>();

    [RavenMultiplatformFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public async Task CanIndexMultipleVectorGeneratedByEtlJs() => await CanIndexMultipleVectorGeneratedByEtlBase<IndexByNamesJs>();

    private async Task CanIndexMultipleVectorGeneratedByEtlBase<TIndex>() where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore();

        string id;
        using (var session = store.OpenSession())
        {
            var dto = new Dto { Names = ["Joe", "Jimmy"] };
            session.Store(dto);
            session.SaveChanges();
            id = dto.Id;
        }

        var index = new TIndex();
        await index.ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(1, nullElements);
        }

        await store.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));
        var etlStatus = Etl.WaitForEtlToComplete(store);
        var (config, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Names", ChunkingOptions = DefaultChunkingOptions }]);
        Assert.True(await etlStatus.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);
        await store.Maintenance.SendAsync(new StartIndexOperation(index.IndexName));
        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<Dto, TIndex>().VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("Joe"), minimumSimilarity: 0.75f)
                .ToList();

            Assert.Single(byVector);
        }

        var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Names", ["Joe", "Jimmy"], id);
        etlStatus.Reset();
        using (var session = store.OpenSession())
        {
            var load = session.Load<Dto>(id);
            load.Names = ["sdklfjklsadjkl;assdjaskll"]; // lets change it to random string just not to have similar vector to previous one
            session.Store(load);
            session.SaveChanges();
        }

        Assert.True(await etlStatus.WaitAsync(DefaultEtlTimeout));
        await Indexes.WaitForIndexingAsync(store);
        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<Dto, TIndex>()
                .VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("Joe"), minimumSimilarity: 0.75f)
                .ToList();

            Assert.Empty(byVector);
        }

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Names", ["sdklfjklsadjkl;assdjaskll"], id);
    }


    [RavenMultiplatformFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public async Task CanIndexVectorFromTwoDifferentEtl() => await CanIndexVectorFromTwoDifferentEtlBase<IndexByFieldTwoFields>();

    [RavenMultiplatformFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public async Task CanIndexVectorFromTwoDifferentEtlJs() => await CanIndexVectorFromTwoDifferentEtlBase<IndexByFieldTwoFieldsJs>();

    private async Task CanIndexVectorFromTwoDifferentEtlBase<TIndex>() where TIndex : AbstractIndexCreationTask, new()
    {
        const string embeddingEtlName = "V1";
        const string embeddingEtlName2 = "V2";
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var store = GetDocumentStore();
        
        string id;
        using (var session = store.OpenSession())
        {
            var dto = new Dto { Name = "Joe", Names = ["Jimmy"] };
            session.Store(dto);
            session.SaveChanges();
            id = dto.Id;
        }

        var index = new TIndex();
        await index.ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(1, nullElements);

            nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector2 == null);
            Assert.Equal(1, nullElements);
        }

        await store.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));
        var etlStatus = Etl.WaitForEtlToComplete(store);
        var (config, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions }], embeddingsGenerationTaskName: embeddingEtlName);
        
        Assert.True(await etlStatus.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);
        AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Name", ["Joe"], id);
        
        await store.Maintenance.SendAsync(new StartIndexOperation(index.IndexName));
        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector2 == null);
            Assert.Equal(1, nullElements);

            nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<Dto, TIndex>()
                .VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("Joe"))
                .ToList();

            Assert.Single(byVector);
        }

        etlStatus.Reset();
        var (config2, connectionString2) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Names", ChunkingOptions = DefaultChunkingOptions }], embeddingsGenerationTaskName: embeddingEtlName2);
        Assert.True(await etlStatus.WaitAsync(DefaultEtlTimeout));
        (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config2);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);
        await Indexes.WaitForIndexingAsync(store);
        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector2 == null);
            Assert.Equal(0, nullElements);

            nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<Dto, TIndex>().VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("Joe"))
                .ToList();
            Assert.Single(byVector);

            byVector = session.Query<Dto, TIndex>().VectorSearch(f => f.WithField(s => s.Vector2),
                    v => v.ByText("Jimmy"))
                .ToList();
            Assert.Single(byVector);
        }

        AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config2.Identifier), new AiConnectionStringIdentifier(connectionString2.Identifier), "Names", ["Jimmy"], id);
    }

    private class IndexByName : AbstractIndexCreationTask<Dto>
    {
        public IndexByName()
        {
            Map = dtos => from dto in dtos
                          select new { Vector = LoadVector("Name", "localaitask") };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class IndexByNameJs : AbstractJavaScriptIndexCreationTask
    {
        public IndexByNameJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('Dtos', function (doc) {{
                return {{
                    Id: id(doc),
                    Vector: loadVector('Name', 'localaitask'),
                }};
            }})"
            };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class IndexByNames : AbstractIndexCreationTask<Dto>
    {
        public IndexByNames()
        {
            Map = dtos => from dto in dtos
                          select new { Vector = LoadVector("Names", "localaitask") };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class IndexByNamesJs : AbstractJavaScriptIndexCreationTask
    {
        public IndexByNamesJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('Dtos', function (doc) {{
                return {{
                    Id: id(doc),
                    Vector: loadVector('Names', 'localaitask'),
                }};
            }})"
            };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }


    private class IndexByFieldTwoFields : AbstractIndexCreationTask<Dto>
    {
        public IndexByFieldTwoFields()
        {
            Map = dtos => from dto in dtos
                          select new
                          {
                              Vector = LoadVector("Name", "v1"),
                              Vector2 = LoadVector("Names", "v2")
                          };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class IndexByFieldTwoFieldsJs : AbstractJavaScriptIndexCreationTask
    {
        public IndexByFieldTwoFieldsJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('Dtos', function (doc) {{
                return {{
                    Id: id(doc),
                    Vector: loadVector('Name', 'v1'),
                    Vector2: loadVector('Names', 'v2')
                }};
            }})"
            };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string[] Names { get; set; }

        public object Vector { get; }
        public object Vector2 { get; }
    }
}

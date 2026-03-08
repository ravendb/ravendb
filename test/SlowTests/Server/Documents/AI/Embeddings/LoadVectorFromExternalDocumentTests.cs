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
using Xunit.Abstractions;
using System.Threading.Tasks;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class LoadVectorFromExternalDocumentTests(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public async Task CanIndexSingleVectorGeneratedByEtlForDifferentDocument() => await CanIndexSingleVectorGeneratedByEtlBase<IndexByName>();
    
    [RavenMultiplatformFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public async Task CanIndexSingleVectorGeneratedByEtlForDifferentDocumentExplicitCollectionName() => await CanIndexSingleVectorGeneratedByEtlBase<IndexByNameExplicitCollection>();

    [RavenMultiplatformFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public async Task CanIndexSingleVectorGeneratedByEtlForDifferentDocumentJs() => await CanIndexSingleVectorGeneratedByEtlBase<IndexByNameJs>();

    private async Task CanIndexSingleVectorGeneratedByEtlBase<TIndex>() where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore();

        string dtoId;
        using (var session = store.OpenSession())
        {
            var dto = new Dto { Name = "Joe" };
            session.Store(dto);
            session.SaveChanges();
            dtoId = dto.Id;
            var queryDto = new QueryDto { DtoId = dto.Id };
            session.Store(queryDto);
            session.SaveChanges();
        }

        var index = new TIndex();
        await index.ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);
        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(1, nullElements);

            var ex = Assert.Throws<InvalidQueryException>(()=> session.Query<QueryDto, TIndex>().VectorSearch(f => f.WithField(s => s.Vector), v => v.ByText("Joe")).ToList());
            Assert.Contains("Couldn't find Embeddings Generation task with 'localaitask' identifier", ex.Message);
        }

        await store.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));
        var etlStatus = Etl.WaitForEtlToComplete(store);
        var (config, connectionString) = AddEmbeddingsGenerationTask(store);
        Assert.True(await etlStatus.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);
        AssertEmbeddingsForPath(store, config, connectionString, "Name", ["Joe"], dtoId);

        await store.Maintenance.SendAsync(new StartIndexOperation(index.IndexName));
        await Indexes.WaitForIndexingAsync(store);
        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<QueryDto, TIndex>()
                .VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("joe"), minimumSimilarity: 0.75f)
                .ToList();

            Assert.Single(byVector);
        }

        var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Joe"], dtoId);
        etlStatus.Reset();
        using (var session = store.OpenSession())
        {
            var load = session.Load<Dto>(dtoId);
            load.Name = "sdklfjklsadjkl;assdjaskll"; // lets change it to random string just not to have similar vector to previous one
            session.Store(load);
            session.SaveChanges();
        }

        Assert.True(await etlStatus.WaitAsync(DefaultEtlTimeout));
        await Indexes.WaitForIndexingAsync(store);
        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<QueryDto, TIndex>()
                .VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("Joe"), minimumSimilarity: 0.75f)
                .ToList();

            Assert.Empty(byVector);
        }

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["sdklfjklsadjkl;assdjaskll"], dtoId);
    }

    [RavenMultiplatformFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public async Task CanIndexMultipleVectorGeneratedByEtlForDifferentDocumentExplicitCollectionName() => await CanIndexMultipleVectorGeneratedByEtlBase<IndexByNamesExplicitCollection>();

    [RavenMultiplatformFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public async Task CanIndexMultipleVectorGeneratedByEtlForDifferentDocument() => await CanIndexMultipleVectorGeneratedByEtlBase<IndexByNames>();
    
    [RavenMultiplatformFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public async Task CanIndexMultipleVectorGeneratedByEtlForDifferentDocumentJs() => await CanIndexMultipleVectorGeneratedByEtlBase<IndexByNamesJs>();

    private async Task CanIndexMultipleVectorGeneratedByEtlBase<TIndex>() where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore();

        string dtoId;
        using (var session = store.OpenSession())
        {
            var dto = new Dto { Names = ["Joe", "Jimmy"] };
            session.Store(dto);
            session.SaveChanges();
            dtoId = dto.Id;
            var dtoQuery = new QueryDto { DtoId = dto.Id };
            session.Store(dtoQuery);
            session.SaveChanges();
        }

        var index = new TIndex();
        await index.ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector == null);
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
            var nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<QueryDto, TIndex>().VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("Joe"), minimumSimilarity: 0.75f)
                .ToList();

            Assert.Single(byVector);
        }

        var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Names", ["Joe", "Jimmy"], dtoId);
        etlStatus.Reset();
        using (var session = store.OpenSession())
        {
            var load = session.Load<Dto>(dtoId);
            load.Names = ["sdklfjklsadjkl;assdjaskll"]; // lets change it to random string just not to have similar vector to previous one
            session.Store(load);
            session.SaveChanges();
        }

        Assert.True(await etlStatus.WaitAsync(DefaultEtlTimeout));
        await Indexes.WaitForIndexingAsync(store);
        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<QueryDto, TIndex>()
                .VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("Joe"), minimumSimilarity: 0.75f)
                .ToList();

            Assert.Empty(byVector);
        }

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Names", ["sdklfjklsadjkl;assdjaskll"], dtoId);
    }
    
    [RavenMultiplatformFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public async Task CanIndexVectorFromTwoDifferentEtlExplicitCollectionName() => await CanIndexVectorFromTwoDifferentEtlBase<IndexByFieldTwoFieldsExplicitCollection>();

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
            var queryDto = new QueryDto { DtoId = dto.Id };
            session.Store(queryDto);
            session.SaveChanges();
        }

        var index = new TIndex();
        await index.ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);

        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(1, nullElements);

            nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector2 == null);
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
            var nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector2 == null);
            Assert.Equal(1, nullElements);

            nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<QueryDto, TIndex>()
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
            var nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector2 == null);
            Assert.Equal(0, nullElements);

            nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<QueryDto, TIndex>().VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("Joe"))
                .ToList();
            Assert.Single(byVector);

            byVector = session.Query<QueryDto, TIndex>().VectorSearch(f => f.WithField(s => s.Vector2),
                    v => v.ByText("Jimmy"))
                .ToList();
            Assert.Single(byVector);
        }

        AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config2.Identifier), new AiConnectionStringIdentifier(connectionString2.Identifier), "Names", ["Jimmy"], id);
    }

    [RavenMultiplatformFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public Task CanLoadVectorsFromTwoDifferentCollections() => CanLoadVectorsFromTwoDifferentCollectionsBase<IndexByNamesFromCurrentAndDifferentCollection>();
    
    [RavenMultiplatformFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector, RavenArchitecture.AllX64)]
    public Task CanLoadVectorsFromTwoDifferentCollectionJs() => CanLoadVectorsFromTwoDifferentCollectionsBase<IndexByNamesFromCurrentAndDifferentCollectionJs>();
    
    private async Task CanLoadVectorsFromTwoDifferentCollectionsBase<TIndex>() where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore();

        string dtoId;
        string queryDtoId;
        using (var session = store.OpenSession())
        {
            var dto = new Dto { Name = "Joe" };
            session.Store(dto);
            session.SaveChanges();
            dtoId = dto.Id;
            var queryDto = new QueryDto { DtoId = dto.Id, Name = "car"};
            session.Store(queryDto);
            session.SaveChanges();
            queryDtoId = queryDto.Id;
        }

        var index = new TIndex();
        await index.ExecuteAsync(store);
        
        await Indexes.WaitForIndexingAsync(store);
        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector == null);
            var nullElementsFromCurrentCollection = session.Query<QueryDto, TIndex>().Count(x => x.Vector2 == null);
            Assert.Equal(1, nullElements);
            Assert.Equal(1, nullElementsFromCurrentCollection);

            var ex = Assert.Throws<InvalidQueryException>(()=> session.Query<QueryDto, TIndex>().VectorSearch(f => f.WithField(s => s.Vector), v => v.ByText("Joe")).ToList());
            var ex2 = Assert.Throws<InvalidQueryException>(()=> session.Query<QueryDto, TIndex>().VectorSearch(f => f.WithField(s => s.Vector2), v => v.ByText("car")).ToList());
            Assert.Contains("Couldn't find Embeddings Generation task with 'localaitask' identifier", ex.Message);
            Assert.Contains("Couldn't find Embeddings Generation task with 'querydtotask' identifier", ex2.Message);
        }

        await store.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));
        var etlStatus = Etl.WaitForEtlToComplete(store);
        var (configDto, connectionStringDto) = AddEmbeddingsGenerationTask(store);
        Assert.True(await etlStatus.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configDto);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);
        AssertEmbeddingsForPath(store, configDto, connectionStringDto, "Name", ["Joe"], dtoId);

        await store.Maintenance.SendAsync(new StartIndexOperation(index.IndexName));
        await Indexes.WaitForIndexingAsync(store);
        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector2 == null);
            Assert.Equal(1, nullElements);
            
            var byVector = session.Query<QueryDto, TIndex>()
                .VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("joe"), minimumSimilarity: 0.75f)
                .ToList();

            Assert.Single(byVector);
            var ex2 = Assert.Throws<InvalidQueryException>(()=> session.Query<QueryDto, TIndex>().VectorSearch(f => f.WithField(s => s.Vector2), v => v.ByText("car")).ToList());
            Assert.Contains("Couldn't find Embeddings Generation task with 'querydtotask' identifier", ex2.Message);
        }

        var aiIntegrationIdentifierForDto = new EmbeddingsGenerationTaskIdentifier(configDto.Identifier);
        var aiConnectionStringIdentifierForDto = new AiConnectionStringIdentifier(connectionStringDto.Identifier);

        AssertEmbeddingsForPath(store, aiIntegrationIdentifierForDto, aiConnectionStringIdentifierForDto, "Name", ["Joe"], dtoId);
        etlStatus.Reset();
        
        
        //Register embedding generation task for QueriesDto
        var (configQueriesDto, connectionStringQueriesDto) = AddEmbeddingsGenerationTask(store, "querydtotask", "secondaiconnection", collectionName: "QueryDtos");
        var aiIntegrationIdentifierForQueriesDto = new EmbeddingsGenerationTaskIdentifier(configQueriesDto.Identifier);
        var aiConnectionStringIdentifierForQueriesDto = new AiConnectionStringIdentifier(connectionStringQueriesDto.Identifier);
        Assert.True(await etlStatus.WaitAsync(DefaultEtlTimeout));
        (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configDto);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);
        AssertEmbeddingsForPath(store, configQueriesDto, connectionStringQueriesDto, "Name", ["car"], queryDtoId);
        etlStatus.Reset();
        await Indexes.WaitForIndexingAsync(store);
        
        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector2 == null);
            Assert.Equal(0, nullElements);
            
            var byVector = session.Query<QueryDto, TIndex>()
                .VectorSearch(f => f.WithField(s => s.Vector2),
                    v => v.ByText("car"), minimumSimilarity: 0.75f)
                .ToList();

            Assert.Single(byVector);
        }
        
        
        using (var session = store.OpenSession())
        {
            var load = session.Load<Dto>(dtoId);
            load.Name = "sdklfjklsadjkl;assdjaskll"; // lets change it to random string just not to have similar vector to previous one
            
            var load2 = session.Load<QueryDto>(queryDtoId);
            load2.Name = "dsafadfae"; // lets change it to random string just not to have similar vector to previous one
            
            session.Store(load);
            session.SaveChanges();
        }

        Assert.True(await etlStatus.WaitAsync(DefaultEtlTimeout));
        await Indexes.WaitForIndexingAsync(store);
        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);
            
            nullElements = session.Query<QueryDto, TIndex>().Count(x => x.Vector2 == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<QueryDto, TIndex>()
                .VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("Joe"), minimumSimilarity: 0.75f)
                .ToList();

            Assert.Empty(byVector);
            
            byVector = session.Query<QueryDto, TIndex>()
                .VectorSearch(f => f.WithField(s => s.Vector2),
                    v => v.ByText("car"), minimumSimilarity: 0.75f)
                .ToList();

            Assert.Empty(byVector);
        }

        AssertEmbeddingsForPath(store, aiIntegrationIdentifierForDto, aiConnectionStringIdentifierForDto, "Name", ["sdklfjklsadjkl;assdjaskll"], dtoId);
        
        AssertEmbeddingsForPath(store, aiIntegrationIdentifierForQueriesDto, aiConnectionStringIdentifierForQueriesDto, "Name", ["dsafadfae"], queryDtoId);
    }

    private class IndexByName : AbstractIndexCreationTask<QueryDto>
    {
        public IndexByName()
        {
            Map = dtos => from dto in dtos
                          select new { Vector = LoadVector<Dto>("Name", "localaitask", dto.DtoId) };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }
    
    private class IndexByNameExplicitCollection : AbstractIndexCreationTask<QueryDto>
    {
        public IndexByNameExplicitCollection()
        {
            Map = dtos => from dto in dtos
                select new { Vector = LoadVector("Name", "localaitask", dto.DtoId, "Dtos") };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class IndexByNameJs : AbstractJavaScriptIndexCreationTask
    {
        public IndexByNameJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('QueryDtos', function (doc) {{
                return {{
                    Id: id(doc),
                    Vector: loadVector('Name', 'localaitask', doc.DtoId, 'Dtos'),
                }};
            }})"
            };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class IndexByNames : AbstractIndexCreationTask<QueryDto>
    {
        public IndexByNames()
        {
            Map = queryDtos => from queryDto in queryDtos
                          select new { Vector = LoadVector<Dto>("Names", "localaitask", queryDto.DtoId) };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }
    
    private class IndexByNamesFromCurrentAndDifferentCollection : AbstractIndexCreationTask<QueryDto>
    {
        public IndexByNamesFromCurrentAndDifferentCollection()
        {
            Map = queryDtos => from queryDto in queryDtos
                let x = "rewrite me"
                select new
                {
                    Ignore = x,
                    Vector = LoadVector<Dto>("Name", "localaitask", queryDto.DtoId),
                    Vector2 = LoadVector("Name", "querydtotask")
                };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }
    
    private class IndexByNamesFromCurrentAndDifferentCollectionJs : AbstractJavaScriptIndexCreationTask
    {
        public IndexByNamesFromCurrentAndDifferentCollectionJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('QueryDtos', function (doc) {{
                return {{
                    Id: id(doc),
                    Vector: loadVector('Name', 'localaitask', doc.DtoId, 'Dtos'),
                    Vector2: loadVector('Name', 'querydtotask')
                }};
            }})"
            };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }
    
    private class IndexByNamesExplicitCollection : AbstractIndexCreationTask<QueryDto>
    {
        public IndexByNamesExplicitCollection()
        {
            Map = queryDtos => from queryDto in queryDtos
                select new { Vector = LoadVector("Names", "localaitask", queryDto.DtoId, "Dtos") };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class IndexByNamesJs : AbstractJavaScriptIndexCreationTask
    {
        public IndexByNamesJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('QueryDtos', function (doc) {{
                return {{
                    Id: id(doc),
                    Vector: loadVector('Names', 'localaitask', doc.DtoId, 'Dtos'),
                }};
            }})"
            };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }


    private class IndexByFieldTwoFields : AbstractIndexCreationTask<QueryDto>
    {
        public IndexByFieldTwoFields()
        {
            Map = queryDtos => from queryDto in queryDtos
                          select new
                          {
                              Vector = LoadVector<Dto>("Name", "v1", queryDto.DtoId),
                              Vector2 = LoadVector<Dto>("Names", "v2", queryDto.DtoId)
                          };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }
    
    private class IndexByFieldTwoFieldsExplicitCollection : AbstractIndexCreationTask<QueryDto>
    {
        public IndexByFieldTwoFieldsExplicitCollection()
        {
            Map = queryDtos => from queryDto in queryDtos
                select new
                {
                    Vector = LoadVector("Name", "v1", queryDto.DtoId, "Dtos"),
                    Vector2 = LoadVector("Names", "v2", queryDto.DtoId, "Dtos")
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
                $@"map('QueryDtos', function (doc) {{
                return {{
                    Id: id(doc),
                    Vector: loadVector('Name', 'v1', doc.DtoId, 'Dtos'),
                    Vector2: loadVector('Names', 'v2', doc.DtoId, 'Dtos')
                }};
            }})"
            };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class QueryDto
    {
        public string Id { get; set; }
        public string DtoId { get; set; }
        
        public object Vector { get; }
        public object Vector2 { get; }
        public string Name { get; set; }
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

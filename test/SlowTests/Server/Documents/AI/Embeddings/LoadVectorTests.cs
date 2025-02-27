using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class LoadVectorTests(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    public void CanIndexSingleVectorGeneratedByEtl() => CanIndexSingleVectorGeneratedByEtlBase<IndexByName>();

    [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    public void CanIndexSingleVectorGeneratedByEtlJs() => CanIndexSingleVectorGeneratedByEtlBase<IndexByNameJs>();

    private void CanIndexSingleVectorGeneratedByEtlBase<TIndex>() where TIndex : AbstractIndexCreationTask, new()
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
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(1, nullElements);
        }

        store.Maintenance.Send(new StopIndexOperation(index.IndexName));
        var etlStatus = Etl.WaitForEtlToComplete(store);
        var (config, connectionString) = RegisterAiIntegration(store);
        etlStatus.Wait(TimeSpan.FromSeconds(10));

        store.Maintenance.Send(new StartIndexOperation(index.IndexName));
        Indexes.WaitForIndexing(store);
WaitForUserToContinueTheTest(store);
        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<Dto, TIndex>()
                .VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("joe"))
                .ToList();
            WaitForUserToContinueTheTest(store);
            Assert.Single(byVector);
        }

        var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["Joe"], id);
        using (var session = store.OpenSession())
        {
            var load = session.Load<Dto>(id);
            load.Name = "sdklfjklsadjkl;assdjaskll"; // lets change it to random string just not to have similar vector to previous one
            session.Store(load);
            session.SaveChanges();
        }

        etlStatus.Reset();
        etlStatus.Wait(TimeSpan.FromSeconds(10));
        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<Dto, TIndex>()
                .VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByEmbedding("Joe"))
                .ToList();

            Assert.Empty(byVector);
        }

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Name", ["sdklfjklsadjkl;assdjaskll"], id);
    }

    [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    public void CanIndexMultipleVectorGeneratedByEtl() => CanIndexMultipleVectorGeneratedByEtlBase<IndexByNames>();

    [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    public void CanIndexMultipleVectorGeneratedByEtlJs() => CanIndexMultipleVectorGeneratedByEtlBase<IndexByNamesJs>();

    private void CanIndexMultipleVectorGeneratedByEtlBase<TIndex>() where TIndex : AbstractIndexCreationTask, new()
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
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(1, nullElements);
        }

        store.Maintenance.Send(new StopIndexOperation(index.IndexName));
        var etlStatus = Etl.WaitForEtlToComplete(store);
        var (config, connectionString) = RegisterAiIntegration(store, embeddingsPaths: ["Names"]);
        etlStatus.Wait(TimeSpan.FromSeconds(10));

        store.Maintenance.Send(new StartIndexOperation(index.IndexName));
        Indexes.WaitForIndexing(store);

        WaitForUserToContinueTheTest(store);

        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<Dto, TIndex>().VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("Joe"))
                .ToList();

            Assert.Single(byVector);
        }

        var aiIntegrationIdentifier = new EmbeddingsGenerationTaskIdentifier(config.Identifier);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connectionString.Identifier);

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Names", ["Joe", "Jimmy"], id);
        using (var session = store.OpenSession())
        {
            var load = session.Load<Dto>(id);
            load.Names = ["sdklfjklsadjkl;assdjaskll"]; // lets change it to random string just not to have similar vector to previous one
            session.Store(load);
            session.SaveChanges();
        }

        etlStatus.Reset();
        etlStatus.Wait(TimeSpan.FromSeconds(10));
        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(0, nullElements);

            var byVector = session.Query<Dto, TIndex>()
                .VectorSearch(f => f.WithField(s => s.Vector),
                    v => v.ByText("Joe"))
                .ToList();

            Assert.Empty(byVector);
        }

        AssertEmbeddingsForPath(store, aiIntegrationIdentifier, aiConnectionStringIdentifier, "Names", ["sdklfjklsadjkl;assdjaskll"], id);
    }


    [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    public void CanIndexVectorFromTwoDifferentEtl() => CanIndexVectorFromTwoDifferentEtlBase<IndexByFieldTwoFields>();

    [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Corax | RavenTestCategory.Vector)]
    public void CanIndexVectorFromTwoDifferentEtlJs() => CanIndexVectorFromTwoDifferentEtlBase<IndexByFieldTwoFieldsJs>();

    private void CanIndexVectorFromTwoDifferentEtlBase<TIndex>() where TIndex : AbstractIndexCreationTask, new()
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
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector == null);
            Assert.Equal(1, nullElements);

            nullElements = session.Query<Dto, TIndex>().Count(x => x.Vector2 == null);
            Assert.Equal(1, nullElements);
        }

        store.Maintenance.Send(new StopIndexOperation(index.IndexName));
        var etlStatus = Etl.WaitForEtlToComplete(store);
        var (config, connectionString) = RegisterAiIntegration(store, embeddingsPaths: ["Name"], embeddingsGenerationTaskName: embeddingEtlName);
        etlStatus.Wait(TimeSpan.FromSeconds(10));
        AssertEmbeddingsForPath(store, new EmbeddingsGenerationTaskIdentifier(config.Identifier), new AiConnectionStringIdentifier(connectionString.Identifier), "Name", ["Joe"], id);


        store.Maintenance.Send(new StartIndexOperation(index.IndexName));
        Indexes.WaitForIndexing(store);

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
        var (config2, connectionString2) = RegisterAiIntegration(store, embeddingsPaths: ["Names"], embeddingsGenerationTaskName: embeddingEtlName2);
        etlStatus.Wait(TimeSpan.FromSeconds(10));

        Indexes.WaitForIndexing(store);
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
                          select new { Vector = LoadVector("localaitask", "Name") };

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
                    Vector: loadVector('localaitask', 'Name'),
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
                          select new { Vector = LoadVector("localaitask", "Names") };

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
                    Vector: loadVector('localaitask','Names'),
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
                              Vector = LoadVector("v1", "Name"),
                              Vector2 = LoadVector("v2", "Names")
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
                    Vector: loadVector('v1','Name'),
                    Vector2: loadVector('v2', 'Names')
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

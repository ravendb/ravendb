using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax.Vectors;

public class VectorIndexingWithMixedFields(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void CanIndexNullWithVector(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Store(new AutoVecDoc(string.Empty, new float[] { .1f, .2f }, null, null, 0f, 0f));
        session.Store(new AutoVecDoc(string.Empty, null, null, null, 0f, 0f));
        session.SaveChanges();

        var result = session.Query<AutoVecDoc>()
            .Customize(x => x.WaitForNonStaleResults())
            .VectorSearch(x => x.WithEmbedding(p => p.Singles), v => v.ByEmbedding([.1f, .2f]))
            .Single();
        Assert.NotNull(result.Singles);
        result = session.Advanced.RawQuery<AutoVecDoc>($"from index 'Auto/AutoVecDocs/ByVector.search(Singles)' where 'vector.search(Singles)' == null").Single();
        Assert.Null(result.Singles);
    }

    [RavenMultiplatformTheory(RavenTestCategory.Vector | RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void CanIndexNullWhenServerGeneratesTheEmbedding(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Store(new AutoVecDoc("animal", null, null, null, 0f, 0f));
        session.Store(new AutoVecDoc(null, null, null, null, 0f, 0f));
        session.SaveChanges();
        var result = session.Query<AutoVecDoc>()
            .Customize(x => x.WaitForNonStaleResults())
            .VectorSearch(x => x.WithText(p => p.Text), v => v.ByText("animal"))
            .Single();
        Assert.NotNull(result.Text);

        result = session.Advanced
            .RawQuery<AutoVecDoc>($"from index 'Auto/AutoVecDocs/ByVector.search(embedding.text(Text))' where 'vector.search(embedding.text(Text))' == null")
            .Single();
        Assert.Null(result.Singles);
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void CannotMixVectorAndTextualValues(Options options) => CannotMixVectorAndTextualValuesBase<TextualWithVectorMixedField>(options);

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void CannotMixVectorAndTextualValuesJs(Options options) => CannotMixVectorAndTextualValuesBase<TextualWithVectorMixedFieldJs>(options);

    private void CannotMixVectorAndTextualValuesBase<TIndex>(Options options)
    where  TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Store(new AutoVecDoc("test", [.1f, .2f], null, null, 0f, 0f));
        session.SaveChanges();
        var index = new TIndex();
        index.Execute(store);
        var errors = WaitForIndexingErrorsOnAnyShard(store, [index.IndexName]);
        Assert.NotNull(errors[0].Errors[0].Error);
        Assert.Contains("tried to index textual value instead", errors[0].Errors[0].Error);
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void CannotMixVectorAndNumericalValues(Options options) => CannotMixVectorAndNumericalValuesBase<NumericalWithVectorMixedField>(options);
    public void CannotMixVectorAndNumericalValuesJs() => CannotMixVectorAndNumericalValuesBase<NumericalWithVectorMixedFieldJs>(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
    private void CannotMixVectorAndNumericalValuesBase<TIndex>(Options options)
    where  TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Store(new AutoVecDoc(null, [.1f, .2f], [2, -3], null, 0f, 0f));
        session.SaveChanges();
        var index = new TIndex();
        index.Execute(store);
        var errors = WaitForIndexingErrorsOnAnyShard(store, [index.IndexName]);
        Assert.NotNull(errors[0].Errors[0].Error);
        Assert.Contains("tried to index numerical value instead", errors[0].Errors[0].Error);
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void CannotMixVectorAndSpatialValues(Options options) => CannotMixVectorAndSpatialValuesBase<SpatialWithVectorMixedField>(options);

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void CannotMixVectorAndSpatialValuesJs(Options options) => CannotMixVectorAndSpatialValuesBase<SpatialWithVectorMixedFieldJs>(options);

    private void CannotMixVectorAndSpatialValuesBase<TIndex>(Options options)
    where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Store(new AutoVecDoc(null, null, [2, -3], null, 0f, 0f));
        session.SaveChanges();
        var index = new TIndex();
        index.Execute(store);
        var errors = WaitForIndexingErrorsOnAnyShard(store, [index.IndexName]);
        Assert.NotNull(errors[0].Errors[0].Error);
        Assert.Contains("tried to index spatial value instead", errors[0].Errors[0].Error);
    }

    private IndexErrors[] WaitForIndexingErrorsOnAnyShard(IDocumentStore store, string[] indexNames = null)
    {
        var databaseRecord = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
        if (databaseRecord.IsSharded == false)
            return Indexes.WaitForIndexingErrors(store, indexNames);

        var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromMinutes(1);
        var sp = Stopwatch.StartNew();
        while (sp.Elapsed < timeout)
        {
            foreach (var shardNumber in databaseRecord.Sharding.Shards.Keys)
            {
                var shardErrors = store.Maintenance.ForShard(shardNumber).Send(new GetIndexErrorsOperation(indexNames));
                var withErrors = shardErrors.Where(e => e.Errors.Length > 0).ToArray();
                if (withErrors.Length > 0)
                    return withErrors;
            }
            Thread.Sleep(32);
        }

        throw new TimeoutException($"Got no index error on any shard for more than {timeout}.");
    }

    private record AutoVecDoc(string Text, float[] Singles, sbyte[] Int8, byte[] Binary, float lat, float lon, string Id = null);

    private class TextualWithVectorMixedField : AbstractIndexCreationTask<AutoVecDoc>
    {
        public TextualWithVectorMixedField()
        {
            Map = docs => from doc in docs
                select new { Vector = doc.Text ?? CreateVector(doc.Singles) };

            Vector("Vector", factory => factory.SourceEmbedding(VectorEmbeddingType.Single).DestinationEmbedding(VectorEmbeddingType.Single));
        }
    }
    
    private class TextualWithVectorMixedFieldJs : AbstractJavaScriptIndexCreationTask
    {
        public TextualWithVectorMixedFieldJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('AutoVecDocs', function (doc) {{
                return {{
                    Vector: doc.Text == null ? createVector(doc.Singles) : doc.Text
                }};
            }})"
            };

            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions { Vector = new VectorOptions() { SourceEmbeddingType = VectorEmbeddingType.Single, DestinationEmbeddingType = VectorEmbeddingType.Single } });
        }
    }

    private class NumericalWithVectorMixedField : AbstractIndexCreationTask<AutoVecDoc>
    {
        public NumericalWithVectorMixedField()
        {
            Map = docs => from doc in docs
                select new { Vector = doc.Int8 ?? CreateVector(doc.Singles) };

            Vector("Vector", factory => factory.SourceEmbedding(VectorEmbeddingType.Single).DestinationEmbedding(VectorEmbeddingType.Single));
        }
    }
    
    private class NumericalWithVectorMixedFieldJs : AbstractJavaScriptIndexCreationTask
    {
        public NumericalWithVectorMixedFieldJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('AutoVecDocs', function (dto) {{
                return {{
                    Vector: dto.Int8 ?? createVector(dto.Singles)
                }};
            }})"
            };

            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions { Vector = new VectorOptions() { SourceEmbeddingType = VectorEmbeddingType.Single, DestinationEmbeddingType = VectorEmbeddingType.Single } });
        }
    }

    private class SpatialWithVectorMixedField : AbstractIndexCreationTask<AutoVecDoc>
    {
        public SpatialWithVectorMixedField()
        {
            Map = docs => from doc in docs
                select new { Vector = doc.Singles == null ? CreateSpatialField(doc.lat, doc.lon) : CreateVector(doc.Singles) };

            Vector("Vector", factory => factory.SourceEmbedding(VectorEmbeddingType.Single).DestinationEmbedding(VectorEmbeddingType.Single));
        }
    }
    
    private class SpatialWithVectorMixedFieldJs : AbstractJavaScriptIndexCreationTask
    {
        public SpatialWithVectorMixedFieldJs()
        {
            Maps = new HashSet<string>()
            { $@"map('AutoVecDocs', function (doc) {{
                return {{
                    Vector: doc.Singles == null ? createSpatialField(doc.lat, doc.lon) : createVector(doc.Singles)
                }};
            }})"
            };

            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions { Vector = new VectorOptions() { SourceEmbeddingType = VectorEmbeddingType.Single, DestinationEmbeddingType = VectorEmbeddingType.Single } });
        }
    }
}

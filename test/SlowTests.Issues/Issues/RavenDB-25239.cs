using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25239(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void CanIndexRavenVectorsFromJsIndexes()
    {
        using var store = GetDocumentStore();
        var csIndex = new Movies_ByVectorFromPhoto();
        csIndex.Execute(store);
        var jsIndex = new Movies_ByVectorFromPhoto_JS();
        jsIndex.Execute(store);

        var vec1 = new RavenVector<float>(new float[]
        {
            0.123f, -0.045f, 0.987f, 0.564f, -0.321f, 0.220f
        });

        var vec2 = new RavenVector<float>(new float[]
        {
            0.456f, -0.056f, 0.123f, 0.899f, -0.765f, 0.881f
        });

        using (var session = store.OpenSession())
        {
            var movie1 = new Movie()
            {
                Id = "movies/1",
                RavenEmbedding = vec1,
                RavenEmbeddings = [vec1, vec2]
            };

            var movie2 = new Movie()
            {
                Id = "movies/2",
                RavenEmbedding = vec2,
                RavenEmbeddings = [vec1, vec2]
            };

            session.Store(movie1);
            session.Store(movie2);
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        var errors = store.Maintenance.Send(new GetIndexErrorsOperation([jsIndex.IndexName, csIndex.IndexName]));
        Assert.Empty(errors[0].Errors);
        Assert.Empty(errors[1].Errors);

        var terms = store.Maintenance.Send(new GetTermsOperation(csIndex.IndexName, "Vector", null, 10));
        var termsJsSingleField = store.Maintenance.Send(new GetTermsOperation(jsIndex.IndexName, "Vector", null, 10));
        var termsJsListField = store.Maintenance.Send(new GetTermsOperation(jsIndex.IndexName, "Vectors", null, 10));
        terms.AsSpan().Sort();
        termsJsListField.AsSpan().Sort();
        termsJsSingleField.AsSpan().Sort();

        Assert.Equal(terms, termsJsListField);
        Assert.Equal(terms, termsJsSingleField);
    }

    private class Movies_ByVectorFromPhoto : AbstractIndexCreationTask<Movie, Movies_ByVectorFromPhoto.IndexEntry>
    {
        public class IndexEntry()
        {
            public object Vector { get; set; }
            public object Vectors { get; set; }
        }

        public Movies_ByVectorFromPhoto()
        {
            Map = movies => from movie in movies
                select new
                {
                    Vector = CreateVector(movie.RavenEmbedding),
                    Vectors = CreateVector(movie.RavenEmbeddings)
                };

            Vector(x => x.Vector,
                builder => builder
                    .SourceEmbedding(VectorEmbeddingType.Single)
                    .DestinationEmbedding(VectorEmbeddingType.Single)
                    .Dimensions(6));

            Vector(x => x.Vectors,
                builder => builder
                    .SourceEmbedding(VectorEmbeddingType.Single)
                    .DestinationEmbedding(VectorEmbeddingType.Single)
                    .Dimensions(6));

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class Movies_ByVectorFromPhoto_JS : AbstractJavaScriptIndexCreationTask
    {
        public Movies_ByVectorFromPhoto_JS()
        {
            Maps = new HashSet<string>()
            {
                @"map('Movies', function (movie) {
                   return {
                        Vector: createVector(movie.RavenEmbedding),
                       Vectors: createVector(movie.RavenEmbeddings)
                   };
            })"
            };

            Fields = new();
            Fields.Add("VectorFromPhoto", new IndexFieldOptions()
            {
                Vector = new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Single,
                    DestinationEmbeddingType = VectorEmbeddingType.Single,
                    Dimensions = 6
                }
            });

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class Movie
    {
        public string Id { get; set; }
        public RavenVector<float> RavenEmbedding { get; set; }
        public List<RavenVector<float>> RavenEmbeddings { get; set; }
    }
}

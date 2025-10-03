using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23466(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void ProjectionOfVectorFieldWillAlwaysResultInValueNotFound() => ProjectionOfVectorFieldWillAlwaysResultInValueNotFoundBase<Index>();
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    private void ProjectionOfVectorFieldWillAlwaysResultInValueNotFoundJs() => ProjectionOfVectorFieldWillAlwaysResultInValueNotFoundBase<IndexJs>();
    
    private void ProjectionOfVectorFieldWillAlwaysResultInValueNotFoundBase<TIndex>()
    where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        string id;
        using (var session = store.OpenSession())
        {
            var dto = new Dto { Vector = new[] { 1f, 2f, 3f } };
            session.Store(dto);
            session.SaveChanges();
            id = dto.Id;
        }
        
        var index = new TIndex();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
        Assert.Null(errors);
        
        using (var session = store.OpenSession())
        {
            var projection = session.Query<ProjectionDto, TIndex>().Select(x => new {x.Id, x.VectorStored}).ToList();
            Assert.Equal(1, projection.Count);
            Assert.Equal(id, projection[0].Id);
            Assert.Equal(null, projection[0].VectorStored);

            var exception = Assert.Throws<InvalidQueryException>(() => session.Query<ProjectionDto, TIndex>()
                .Customize(c => c.Projection(ProjectionBehavior.FromIndexOrThrow))
                .Select(x => new { Id = RavenQuery.Id(x), Test = x.VectorStored })
                .ToList());
            
            Assert.Contains($"Could not extract field 'VectorStored' from index '{index.IndexName}', because index does not contain such", exception.Message);
            var result = session.Query<ProjectionDto, TIndex>()
                .Select(x => new { Id = RavenQuery.Id(x), Test = x.VectorStored })
                .ToList();
            Assert.Equal(1, result.Count);
            Assert.Equal(id, result[0].Id);
            Assert.Equal(null, result[0].Test);
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public object Vector { get; set; }
    }

    private class ProjectionDto
    {
        public string Id { get; set; }
        public float[] VectorStored { get; set; }
    }

    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = dtos => from dto in dtos
                select new { VectorStored = CreateVector((IEnumerable<float>)dto.Vector) };
        }
    }
    
    private class IndexJs : AbstractJavaScriptIndexCreationTask
    {
        public IndexJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('Dtos', function (dto) {{
                return {{
                    VectorStored: createVector(dto.Vector)
                }};
            }})"
            };
        }
    }
}

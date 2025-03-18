using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23457 : RavenTestBase
{
    public RavenDB_23457(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanShowIndexRawEntryWhenVectorSearchIsInsideWhereClause(Options options)
    => CanShowIndexRawEntryWhenVectorSearchIsInsideWhereClauseBase<DummyIndex>(options);
    
    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Vector)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanShowIndexRawEntryWhenVectorSearchIsInsideWhereClauseJs(Options options)
        => CanShowIndexRawEntryWhenVectorSearchIsInsideWhereClauseBase<DummyIndexJs>(options);
    
    private void CanShowIndexRawEntryWhenVectorSearchIsInsideWhereClauseBase<TIndex>(Options options)
    where TIndex : AbstractIndexCreationTask, new()
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() 
                { 
                    Text = "Cat has brown eyes.", 
                    Text2 = "Car has brown eyes.", 
                    Text3 = null, 
                    TextArr = 
                    [
                        "Cat has brown eyes.",
                        "Car has brown eyes."
                    ]
                };
                
                var dto2 = new Dto()
                {
                    Text = "Apple usually is red.",
                    Text2 = "Mouse usually is red.",
                    Text3 = null,
                    TextArr =
                    [
                        "Apple usually is red.",
                        "Mouse usually is red."
                    ]
                };
                
                session.Store(dto1);
                session.Store(dto2);
                
                session.SaveChanges();
                
                var index = new TIndex();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
                Assert.Null(errors);
                
                
                QueryCommand queryCommand = new QueryCommand((InMemoryDocumentSessionOperations)session, new IndexQuery
                {
                    Query = $"from index '{index.IndexName}' where vector.search(Vector, 'ddd', 0.1)"
                }, metadataOnly: false, indexEntriesOnly: true);
                
                session.Advanced.RequestExecutor.Execute(queryCommand, session.Advanced.Context, session.Advanced.SessionInfo);
                
                QueryResult result = queryCommand.Result;
                
                Assert.Equal(2, result.Results.Length);
            }
        }        
    }
    
    private class Dto
    {
        public string Id { get; set; }
        public string Text { get; set; }
        public string Text2 { get; set; }
        public string Text3 { get; set; }
        public string[] TextArr { get; set; }
    }

    private class DummyIndex : AbstractIndexCreationTask<Dto>
    {
        public DummyIndex()
        {
            Map = dtos => from dto in dtos
                let x = new List<string> { dto.Text, dto.Text2 }
                select new
                {
                    Id = dto.Id,
                    Vector = CreateVector(x.Concat(dto.TextArr)),
                    Vector2 = CreateVector(dto.TextArr.Append(dto.Text2)),
                    Vector3 = CreateVector(dto.TextArr)
                };
        }
    }
    
    private class DummyIndexJs : AbstractJavaScriptIndexCreationTask
    {
        public DummyIndexJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('Dtos', function (dto) {{
                return {{
                    Id: id(dto),         
                    Vector: createVector([dto.Text, dto.Text2].concat(dto.TextArr)),
                    Vector2: createVector(dto.TextArr.concat([dto.Text2])),
                    Vector3: createVector(dto.TextArr)
                }};
            }})"
            };
        }
    }
}

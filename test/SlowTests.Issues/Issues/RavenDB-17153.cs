using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17153: RavenTestBase
{
    public RavenDB_17153(ITestOutputHelper output) : base(output)
    {
        
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void CanLoadMultipleDocumentsUsingEnumerableArgument()
    {
        using var store = GetDocumentStore();

        using (var session = store.OpenSession())
        {
            session.Store(new Document { Id = "documents/1", OtherIds = new Dictionary<string, string> { ["other"] = "documents/2" } });
            session.Store(new Document { Id = "documents/2", OtherIds = new Dictionary<string, string>() });
            session.Store(new Document { Id = "documents/3", OtherIds = new Dictionary<string, string>() { ["other1"] = "documents/1", ["other2"] = "documents/2"}});

            session.SaveChanges();
            
            var query = session.Query<Document>();
            
            var projectionWithValues = from doc in query
                let related = RavenQuery.Load<Document>(doc.OtherIds.Values)
                select new
                {
                    Id = doc.Id,
                    Others = doc.OtherIds.ToDictionary(x => x.Key, x => x.Value)
                };
            
            
            var resultsWithValues = projectionWithValues.ToArray();
            
            Assert.Equal(resultsWithValues[0].Others["other"], "documents/2");
            Assert.Equal(resultsWithValues[2].Others["other1"], "documents/1");
            Assert.Equal(resultsWithValues[2].Others["other2"], "documents/2");
            
            var projectionWithSelect = from doc in query
                let related = RavenQuery.Load<Document>(doc.OtherIds.Select(x => x.Value))
                select new
                {
                    Id = doc.Id,
                    Others = doc.OtherIds
                };
            
            var resultsWithSelect = projectionWithSelect.ToArray();
            
            Assert.Equal(resultsWithSelect[0].Others["other"], "documents/2");
            Assert.Equal(resultsWithSelect[2].Others["other1"], "documents/1");
            Assert.Equal(resultsWithSelect[2].Others["other2"], "documents/2");
        }
    }

    private class Document
    { 
        public string Id { get; set;}
        public Dictionary<string, string> OtherIds { get; set; }
    }
}

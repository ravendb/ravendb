using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Linq;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17289 : RavenTestBase
    {
        private readonly DateTime _dateNow;
        private string _indexName = "NestedJson";
        public RavenDB_17289(ITestOutputHelper output) : base(output)
        {
            _dateNow = new DateTime(2012, 12, 12);
        }
        
        [Fact]
        public void CanQueryOnComplexWithDates()
        {
            using (var store = GetDocumentStore())
            {
                using var session = store.OpenSession();

                var document1 = new TestDocument
                {
                    Date = _dateNow,
                    List = new List<TestDocument>
                    {
                        new TestDocument() { Date = _dateNow }, new TestDocument() { Date = _dateNow.AddDays(2) }
                    },
                    Dictionary = new Dictionary<string, TestDocument>
                    {
                        { "1", new TestDocument() { Date = _dateNow } }, { "2", new TestDocument() { Date = _dateNow.AddDays(2) } }
                    },
                    Dictionary2 = new Dictionary<string, string> { { "1", "a" } }
                    , DateDictionary = new()
                    {
                        {"1", _dateNow}
                    }
                };
                session.Store(document1);
                session.SaveChanges();
                CreateIndex(store);
                Indexes.WaitForIndexing(store);

                using var session2 = store.OpenSession();
                var q1 = session2.Query<TestDocument>().Where(x => x.List.Any(y => y.Date == _dateNow));
                var document1Loaded = q1.ToList();
                Assert.Equal(1, document1Loaded.Count);

                //querying on complex object nested in Dictionary requires static index
                var q2 = session2.Query<TestDocument>(_indexName).Where(x => x.Date == _dateNow);
                var document2Loaded = q2.ToList();
                Assert.Equal(1, document2Loaded.Count);
                
                var q3 = session2.Query<TestDocument>().Where(x => x.Dictionary2.Any(y => y.Value == "a"));
                var document3Loaded = q3.ToList();
                Assert.Equal(1, document3Loaded.Count);
                
                var q4 = session2.Query<TestDocument>().Where(x => x.Dictionary2.Any(y => y.Value == "a"));
                var document4Loaded = q4.ToList();
                Assert.Equal(1, document4Loaded.Count);
            }
        }

        private static void CreateIndex(DocumentStore store)
        {
            store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
            {
                Name = "NestedJson",
                Maps = { @"from doc in docs.TestDocuments 
                    select new                      
                    {                               
                    Date = AsJson(doc.Dictionary).Select(x => x.Value.Date) 
                }" },
            }}));
        }
        
        private class TestDocument
        {
            public string Value { get; set; }
            public DateTime Date { get; set; }
            public Dictionary<string, DateTime> DateDictionary { get; set; }
            public List<TestDocument> List { get; set; }
            public Dictionary<string, TestDocument> Dictionary { get; set; }
            public Dictionary<string, string> Dictionary2 { get; set; }
        }
    }
}

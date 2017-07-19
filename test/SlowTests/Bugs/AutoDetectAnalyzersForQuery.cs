//-----------------------------------------------------------------------
// <copyright file="AutoDetectAnalyzersForQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using FastTests;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Bugs
{
    public class AutoDetectAnalyzersForQuery : RavenTestBase
    {
        [Fact]
        public void WillDetectAnalyzerAutomatically()
        {
            using(var store = GetDocumentStore())
            {
                                                
                store.Admin.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs select new { doc.Name}"},
                    Name = "test",
                    Fields = {{"Name", new IndexFieldOptions { Indexing = FieldIndexing.NotAnalyzed}}}
                }}));

                using (var session = store.OpenSession())
                {
                    session.Store(new Foo{Name = "Ayende"});

                    session.SaveChanges();
                }

                using(var session = store.OpenSession())
                {
                    var foos = session.Advanced.DocumentQuery<Foo>("test")
                        .WhereLucene("Name", "Ayende")
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.NotEmpty(foos);
                }
            }
        }

        private class Foo
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
        
    }
}

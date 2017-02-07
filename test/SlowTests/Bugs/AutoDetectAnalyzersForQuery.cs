//-----------------------------------------------------------------------
// <copyright file="AutoDetectAnalyzersForQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using FastTests;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using System.Linq;
using Xunit;

namespace SlowTests.Bugs
{
    public class AutoDetectAnalyzersForQuery : RavenNewTestBase
    {
        [Fact]
        public void WillDetectAnalyzerAutomatically()
        {
            using(var store = GetDocumentStore())
            {
                                                
                store.Admin.Send(new PutIndexOperation("test", new IndexDefinition
                {
                    Maps = { "from doc in docs select new { doc.Name}"} ,
                    Fields = {{"Name", new IndexFieldOptions { Indexing = FieldIndexing.NotAnalyzed}}}
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new Foo{Name = "Ayende"});

                    session.SaveChanges();
                }

                using(var session = store.OpenSession())
                {
                    var foos = session.Advanced.DocumentQuery<Foo>("test")
                        .Where("Name:Ayende")
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

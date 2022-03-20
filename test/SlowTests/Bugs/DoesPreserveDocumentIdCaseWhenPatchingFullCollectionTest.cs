// -----------------------------------------------------------------------
//  <copyright file="DoesPreserveDocumentIdCaseWhenPatchingFullCollectionTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class DoesPreserveDocumentIdCaseWhenPatchingFullCollectionTest : RavenTestBase
    {
        public DoesPreserveDocumentIdCaseWhenPatchingFullCollectionTest(ITestOutputHelper output) : base(output)
        {
        }

        private class FooBar
        {
            public string Name { get; set; }
        }

        [Fact]
        public void DoesPreserveDocumentIdCaseWhenPatchingFullCollection()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = { "from doc in docs select new { doc.Name }" },
                    Name = "test"
                }));

                string documentId;
                using (var session = store.OpenSession())
                {
                    var d = new FooBar();
                    session.Store(d);
                    session.SaveChanges();

                    documentId = session.Advanced.GetDocumentId(d);
                }

                using (var session = store.OpenSession())
                {
                    var d = session.Load<FooBar>(documentId);

                    //Demonstrates that RavenDb stores a case-sensitive document id somewhere
                    Assert.Equal(documentId, session.Advanced.GetDocumentId(d));
                }
                

                Indexes.WaitForIndexing(store);

                store.Operations.Send(new PatchByQueryOperation(new IndexQuery { Query = "FROM INDEX 'test' UPDATE { var theid = id(this); this.Name = theid ; }" })).WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    var d = session.Load<FooBar>(documentId);
                    Assert.Equal("FooBars/1-A", d.Name);
                }
            }
        }
    }
}

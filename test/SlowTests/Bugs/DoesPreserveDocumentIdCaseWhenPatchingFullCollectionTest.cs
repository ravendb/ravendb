// -----------------------------------------------------------------------
//  <copyright file="DoesPreserveDocumentIdCaseWhenPatchingFullCollectionTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Documents;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Bugs
{
    public class DoesPreserveDocumentIdCaseWhenPatchingFullCollectionTest : RavenNewTestBase
    {
        private class FooBar
        {
            public string Name { get; set; }
        }

        [Fact]
        public void DoesPreserveDocumentIdCaseWhenPatchingFullCollection()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation("test", new IndexDefinition
                {
                    Maps = { "from doc in docs select new { doc.Name }" },
                }));

                string documentId = null;
                using (var session = store.OpenSession())
                {
                    var d = new FooBar() { };
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

                string script = @"  var id = __document_id; this.Name = id;";

                WaitForIndexing(store);

                store.Operations.Send(new PatchByIndexOperation("test", new IndexQuery(store.Conventions), new PatchRequest { Script = script } , new QueryOperationOptions())).WaitForCompletion();

                using (var session = store.OpenSession())
                {
                    var d = session.Load<FooBar>(documentId);
                    Assert.Equal("FooBars/1", d.Name);
                }
            }
        }
    }
}

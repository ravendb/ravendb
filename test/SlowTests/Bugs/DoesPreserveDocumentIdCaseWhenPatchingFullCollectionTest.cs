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

namespace SlowTests.Bugs
{
    public class DoesPreserveDocumentIdCaseWhenPatchingFullCollectionTest : RavenTestBase
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
                store.Admin.Send(new PutIndexesOperation(new IndexDefinition
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

                const string script = @"  var id = __document_id; this.Name = id;";

                WaitForIndexing(store);

                store.Operations.Send(new PatchByIndexOperation(new IndexQuery { Query = "FROM INDEX 'test'" }, new PatchRequest { Script = script }, new QueryOperationOptions())).WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    var d = session.Load<FooBar>(documentId);
                    Assert.Equal("FooBars/1-A", d.Name);
                }
            }
        }
    }
}

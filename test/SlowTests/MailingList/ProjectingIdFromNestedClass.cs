// -----------------------------------------------------------------------
//  <copyright file="ProjectingIdFromNestedClass.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class ProjectingIdFromNestedClass : RavenTestBase
    {
        private class Document
        {
            public string Id
            {
                get;
                set;
            }
        }

        private class Documents_TestIndex : AbstractIndexCreationTask<Document>
        {
            public class Result
            {
                public string Id
                {
                    get;
                    set;
                }
            }

            public Documents_TestIndex()
            {
                Map = docs => from d in docs
                              select new
                              {
                                  d.Id
                              };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        [Fact]
        public void TestSelectFields()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new Documents_TestIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new Document());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                                       .DocumentQuery<Document, Documents_TestIndex>()
                                       .WaitForNonStaleResults()
                                       .SelectFields<Documents_TestIndex.Result>()
                                       .ToList();

                    Assert.True(query.All(d => !string.IsNullOrEmpty(d.Id)));
                }
            }
        }
    }
}

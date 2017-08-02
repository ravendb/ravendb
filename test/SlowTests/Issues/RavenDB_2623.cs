// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2623.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2623 : RavenTestBase
    {
        private class People_ByName : AbstractIndexCreationTask<Person>
        {
            public People_ByName()
            {
                Map = people => from person in people select new { person.Name };
            }
        }

        [Fact]
        public void WaitForCompletionShouldThrowWhenOperationHasFaulted()
        {
            using (var store = GetDocumentStore())
            {
                store
                    .Admin
                    .Send(new StopIndexingOperation());

                new People_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 100; i++)
                    {
                        session.Store(new Person { Name = Guid.NewGuid().ToString() });
                    }

                    session.SaveChanges();
                }

                var e = Assert.Throws<RavenException>(() => store
                    .Operations
                    .Send(new DeleteByIndexOperation(new IndexQuery { Query = $"FROM INDEX '{new People_ByName().IndexName}'" }, options: null)).WaitForCompletion(TimeSpan.FromSeconds(15)));

                Assert.Contains("Cannot perform bulk operation. Query is stale.", e.Message);
            }
        }
    }
}

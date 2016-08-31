// -----------------------------------------------------------------------
//  <copyright file="ProjectionShouldNotLoadDocument.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Json.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class ProjectionShouldNotLoadDocument : RavenTestBase
    {
        [Fact]
        public void WhenProjecting()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.Put("FOO", null, new RavenJObject { { "Name", "Ayende" } }, new RavenJObject { { Constants.Headers.RavenEntityName, "Foos" } });
                WaitForIndexing(store);
                var result = store.DatabaseCommands.Query("dynamic/Foos", new IndexQuery
                {
                    FieldsToFetch = new[] { "Name" }
                });

                // if this is lower case, then we loaded this from the index, not from the db
                Assert.Equal("foo", result.Results[0].Value<string>(Constants.Indexing.Fields.DocumentIdFieldName));
            }
        }
    }
}

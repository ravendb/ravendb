// -----------------------------------------------------------------------
//  <copyright file="ProjectionShouldNotLoadDocument.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using FastTests;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Indexes;
using Raven.Client.Indexing;
using Raven.Json.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class ProjectionShouldNotLoadDocument : RavenTestBase
    {
        private class Index1 : AbstractIndexCreationTask
        {
            public override string IndexName { get; } = "Index1";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { "from foo in docs select new { Name = foo.Name }" },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        {
                            "Name",
                            new IndexFieldOptions
                            {
                                Storage = FieldStorage.Yes
                            }
                        }
                    }
                };
            }
        }

        [Fact]
        public void WhenProjecting()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.Put("FOO", null, new RavenJObject { { "Name", "Ayende" } }, new RavenJObject { { Constants.Headers.RavenEntityName, "Foos" } });

                var result = store.DatabaseCommands.Query("dynamic", new IndexQuery
                {
                    FieldsToFetch = new[] { "Name" },
                    Query = "Name:Ayende"
                });

                // if this is upper case, then we loaded this from the db, because we used Auto-Index that is not storing fields
                Assert.Equal("FOO", result.Results[0].Value<string>(Constants.Indexing.Fields.DocumentIdFieldName));
                Assert.True(result.IndexName.StartsWith("Auto"));

                new Index1().Execute(store);
                WaitForIndexing(store);

                result = store.DatabaseCommands.Query("Index1", new IndexQuery
                {
                    FieldsToFetch = new[] { "Name" },
                    Query = "Name:Ayende"
                });

                // if this is lower case, then we loaded this from the index, not from the db, because w used Static-Index with stored field
                Assert.Equal("foo", result.Results[0].Value<string>(Constants.Indexing.Fields.DocumentIdFieldName));
                Assert.True(result.IndexName.StartsWith("Index1"));
            }
        }
    }
}

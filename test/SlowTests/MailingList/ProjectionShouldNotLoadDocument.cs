// -----------------------------------------------------------------------
//  <copyright file="ProjectionShouldNotLoadDocument.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Sparrow.Json;
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
                using (var commands = store.Commands())
                {
                    commands.Put("FOO", null, new { Name = "Ayende" }, new Dictionary<string, object> { { Constants.Documents.Metadata.Collection, "Foos" } });

                    var result = commands.Query(new IndexQuery { Query = "SELECT Name FROM @AllDocs WHERE NAME = 'Ayende'" });

                    // if this is upper case, then we loaded this from the db, because we used Auto-Index that is not storing fields
                    var json = (BlittableJsonReaderObject)result.Results[0];
                    string documentId;
                    Assert.True(json.TryGet(Constants.Documents.Indexing.Fields.DocumentIdFieldName, out documentId));
                    Assert.Equal("FOO", documentId);
                    Assert.True(result.IndexName.StartsWith("Auto"));

                    new Index1().Execute(store);
                    WaitForIndexing(store);

                    result = commands.Query(new IndexQuery { Query = "SELECT Name FROM INDEX 'Index1' WHERE Name = 'Ayende'" });

                    // if this is lower case, then we loaded this from the index, not from the db, because w used Static-Index with stored field
                    json = (BlittableJsonReaderObject)result.Results[0];
                    Assert.True(json.TryGet(Constants.Documents.Indexing.Fields.DocumentIdFieldName, out documentId));
                    Assert.Equal("foo", documentId);
                    Assert.True(result.IndexName.StartsWith("Index1"));
                }
            }
        }
    }
}

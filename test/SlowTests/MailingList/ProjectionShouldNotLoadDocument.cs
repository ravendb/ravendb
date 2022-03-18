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
using Raven.Client.Extensions;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class ProjectionShouldNotLoadDocument : RavenTestBase
    {
        public ProjectionShouldNotLoadDocument(ITestOutputHelper output) : base(output)
        {
        }

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

                    var result = commands.Query(new IndexQuery { Query = "FROM @all_docs WHERE Name = 'Ayende' SELECT Name" });

                    // if this is upper case, then we loaded this from the db, because we used Auto-Index that is not storing fields
                    var json = (BlittableJsonReaderObject)result.Results[0];
                    var metadata = json.GetMetadata();
                    Assert.True(metadata.TryGet(Constants.Documents.Metadata.Id, out string documentId));
                    Assert.Equal("FOO", documentId);
                    Assert.True(result.IndexName.StartsWith("Auto"));

                    new Index1().Execute(store);
                    Indexes.WaitForIndexing(store);

                    result = commands.Query(new IndexQuery { Query = "FROM INDEX 'Index1' WHERE Name = 'Ayende' SELECT Name" });

                    // if this is lower case, then we loaded this from the index, not from the db, because w used Static-Index with stored field
                    json = (BlittableJsonReaderObject)result.Results[0];
                    metadata = json.GetMetadata();
                    Assert.True(metadata.TryGet(Constants.Documents.Metadata.Id, out documentId));
                    Assert.Equal("foo", documentId);
                    Assert.True(result.IndexName.StartsWith("Index1"));
                }
            }
        }
    }
}

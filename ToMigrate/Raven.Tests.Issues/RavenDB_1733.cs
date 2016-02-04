// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1733.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Database.Smuggler.Embedded;
using Raven.Json.Linq;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Streams;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1733 : RavenTest
    {
        private const string EmptyTransform = @"function(doc) {
                        return doc;
                    }";

        [Fact]
        public async Task SmugglerTransformShouldRecognizeNumericPropertiesEvenThoughTheyHaveTheSameNames()
        {
            using (var stream = new MemoryStream())
            {
                var testObject = new RavenJObject
                {
                    {"Range", new RavenJArray {new RavenJObject {{"Min", 2.4}}}},
                    {"Min", 1}
                };

                using (var store = NewDocumentStore())
                {
                    store.DatabaseCommands.Put("docs/1", null, testObject, new RavenJObject());

                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions
                        {
                            TransformScript = EmptyTransform
                        }, 
                        new DatabaseSmugglerEmbeddedSource(store.DocumentDatabase), 
                        new DatabaseSmugglerStreamDestination(stream));

                    await smuggler.ExecuteAsync();
                }

                stream.Position = 0;

                using (var store = NewDocumentStore())
                {
                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions
                        {
                            TransformScript = EmptyTransform
                        },
                        new DatabaseSmugglerStreamSource(stream), 
                        new DatabaseSmugglerEmbeddedDestination(store.DocumentDatabase));

                    await smuggler.ExecuteAsync();

                    var doc = store.DatabaseCommands.Get("docs/1").DataAsJson;
                    Assert.NotNull(doc);
                    Assert.Equal(testObject["Min"].Type, doc["Min"].Type);
                    Assert.Equal(((RavenJObject)((RavenJArray)testObject["Range"])[0])["Min"].Type, ((RavenJObject)((RavenJArray)doc["Range"])[0])["Min"].Type);

                    Assert.True(RavenJToken.DeepEquals(testObject, doc));
                }
            }
        }
    }
}

using System.Collections.Concurrent;
using System.Collections.Generic;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Session;
using Raven.Client.Json;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10147 : RavenTestBase
    {
        private class Entity
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public IEnumerable<string> Tags { get; set; } = new string[0];
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Entity
                    {
                        Id = "transactions/248738",
                        Name = "Whatever",
                        Tags = new string[] {"files/178996"}
                    }, "items/1");

                    session.SaveChanges();
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new Entity
                    {
                        Id = "transactions/248738",
                        Name = "Whatever",
                        Tags = new string[] {"files/178996"}
                    }, "items/2");
                }

                using (var commands = store.Commands())
                {
                    var item1 = commands.Get("items/1");
                    var item2 = commands.Get("items/2");

                    var changes = new ConcurrentDictionary<string, DocumentsChanges[]>();
                    var changed = BlittableOperation.EntityChanged(item1.BlittableJson, new DocumentInfo
                    {
                        IsNewDocument = false,
                        Document = item2.BlittableJson
                    }, changes);
                    
                    Assert.False(changed); // EntityChanged is not comparing @metadata
                    Assert.Equal(0, changes.Count);

                    var metadata1 = (BlittableJsonReaderObject)item1.BlittableJson[Constants.Documents.Metadata.Key];
                    var metadata2 = (BlittableJsonReaderObject)item2.BlittableJson[Constants.Documents.Metadata.Key];
                    
                    Assert.Equal(metadata1.Count, metadata2.Count);
                    Assert.Equal(metadata1[Constants.Documents.Metadata.Collection], metadata2[Constants.Documents.Metadata.Collection]);
                    Assert.Equal(metadata1[Constants.Documents.Metadata.RavenClrType], metadata2[Constants.Documents.Metadata.RavenClrType]);
                }
            }
        }
    }
}

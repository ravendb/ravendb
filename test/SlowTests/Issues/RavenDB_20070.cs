using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20070 : RavenTestBase
{
    public RavenDB_20070(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void Will_Throw_When_Deserializing_Blacklisted_Type()
    {
        var binder = new DefaultRavenSerializationBinder();
        binder.RegisterForbiddenNamespace("MyNamespace");

        using (var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = s =>
            {
                s.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                {
                    CustomizeJsonDeserializer = deserializer => deserializer.SerializationBinder = binder
                };
            }
        }))
        {
            using (var commands = store.Commands())
            {
                var json = "{ 'Item': { '$type': 'System.Xml.XmlDocument, System.Xml' } }";
                var blittableJson = commands.Context.Sync.ReadForMemory(json, "entries/1");
                commands.Put("entries/1", null, blittableJson, new Dictionary<string, object> { { "@collection", "Entries" } });
            }

            using (var session = store.OpenSession())
            {
                var e = Assert.Throws<InvalidOperationException>(() => session.Load<Entry>("entries/1"));
                Assert.Contains("blacklist", e.ToString());
            }

            using (var session = store.OpenSession())
            {
                var e = Assert.Throws<InvalidOperationException>(() => session.Query<Entry>().ToList());
                Assert.Contains("blacklist", e.ToString());
            }
        }

        var e2 = Assert.Throws<InvalidOperationException>(() => binder.RegisterForbiddenNamespace("MyNamespace2"));
        Assert.Contains("binder was already used", e2.Message);
    }

    private class Entry
    {
        public object Item { get; set; }
    }
}

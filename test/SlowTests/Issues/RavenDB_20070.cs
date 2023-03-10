using System;
using FastTests;
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
        using (var store = GetDocumentStore())
        {
            using (var commands = store.Commands())
            {
                var json = "{ 'Item': { '$type': 'System.Xml.XmlDocument, System.Xml' } }";
                var blittableJson = commands.Context.Sync.ReadForMemory(json, "entries/1");
                commands.Put("entries/1", null, blittableJson);
            }

            using (var session = store.OpenSession())
            {
                var e = Assert.Throws<InvalidOperationException>(() => session.Load<Entry>("entries/1"));
                Assert.Contains("blacklist", e.ToString());
            }
        }
    }

    private class Entry
    {
        public object Item { get; set; }
    }
}

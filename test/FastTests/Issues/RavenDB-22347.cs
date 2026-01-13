using System.Collections.Generic;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_22347 : RavenTestBase
    {
        public RavenDB_22347(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryValueWithIndexerAndKeyFromObjectProperty()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Values = new Dictionary<string, string>
                    {
                        { "Key1", "Value1" },
                        { "Key2", "Value2" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            var obj = new { Key = "Key1" };

            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<Document, string>("docs/1", x => x.Values[obj.Key], "UpdatedValue");
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));

                Assert.True(values.TryGet("Key1", out string value));
                Assert.Equal("UpdatedValue", value);
                Assert.True(values.TryGet("Key2", out value));
                Assert.Equal("Value2", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryValueWithIndexerAndKeyFromVariable()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Values = new Dictionary<string, string>
                    {
                        { "Key1", "Value1" },
                        { "Key2", "Value2" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            var key = "Key1";

            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<Document, string>("docs/1", x => x.Values[key], "UpdatedValue");
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));

                Assert.True(values.TryGet("Key1", out string value));
                Assert.Equal("UpdatedValue", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryWithAdderAndKeyFromObjectProperty()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Values = new Dictionary<string, string>
                    {
                        { "Key1", "Value1" },
                        { "Key2", "Value2" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            var keyHolder = new KeyHolder { Key = "Key3" };

            using (var session = store.OpenSession())
            {
                var doc = session.Load<Document>("docs/1");
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Add(keyHolder.Key, "Value3"));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(3, values.Count);

                Assert.True(values.TryGet("Key3", out string value));
                Assert.Equal("Value3", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanRemoveDictionaryKeyWithAdderAndKeyFromObjectProperty()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Values = new Dictionary<string, string>
                    {
                        { "Key1", "Value1" },
                        { "Key2", "Value2" },
                        { "Key3", "Value3" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            var keyHolder = new KeyHolder { Key = "Key2" };

            using (var session = store.OpenSession())
            {
                var doc = session.Load<Document>("docs/1");
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Remove(keyHolder.Key));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(2, values.Count);

                Assert.True(values.TryGet("Key1", out string _));
                Assert.True(values.TryGet("Key3", out string _));
                Assert.False(values.TryGet("Key2", out string _));
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryWithKeyContainingSpecialCharacters()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Values = new Dictionary<string, string>
                    {
                        { "Key1", "Value1" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            var specialKeys = new[] { "key-with-dash", "key with space", "key.with", "key\"with\"quotes" };

            foreach (var key in specialKeys)
            {
                using (var session = store.OpenSession())
                {
                    var doc = session.Load<Document>("docs/1");
                    session.Advanced.Patch(doc, x => x.Values, dict => dict.Add(key, $"Value for {key}"));
                    session.SaveChanges();
                }
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(5, values.Count);

                foreach (var key in specialKeys)
                {
                    Assert.True(values.TryGet(key, out string value), $"Key '{key}' should exist");
                    Assert.Equal($"Value for {key}", value);
                }
            }
        }

        private class Document
        {
            public Dictionary<string, string> Values { get; set; }
        }

        private class KeyHolder
        {
            public string Key { get; set; }
        }
    }
}

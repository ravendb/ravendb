using System.Collections.Generic;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_22347 : RavenTestBase
    {
        public RavenDB_22347(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [InlineData("Key1")]
        [InlineData("key-with-dash")]
        [InlineData("key with space")]
        [InlineData("key.with.dot")]
        [InlineData("back\\slash")]
        [InlineData("ke\"y1")]
        [InlineData("it's a \"test\"\\path.with-dashes and spaces")]
        [InlineData("line1\nline2")]
        [InlineData("col1\tcol2")]
        [InlineData("")]
        [InlineData("\r\n")]
        [InlineData("\u2028")]
        [InlineData("\u2029")]
        [InlineData("\u3712")]
        [InlineData("\ud83d\ude00")]
        [InlineData("key \ud83d\udd11 with emoji")]
        public void CanPatchDictionaryViaIndexerWithVariousKeys(string key)
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Values = new Dictionary<string, string>
                    {
                        { key, "OriginalValue" },
                        { "Untouched", "Untouched" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<Document, string>("docs/1", x => x.Values[key], "Updated");
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.True(values.TryGet(key, out string value), $"Key '{key}' should exist");
                Assert.Equal("Updated", value);
                Assert.True(values.TryGet("Untouched", out value));
                Assert.Equal("Untouched", value);
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [InlineData("NewKey")]
        [InlineData("key-with-dash")]
        [InlineData("key with space")]
        [InlineData("key.with.dot")]
        [InlineData("back\\slash")]
        [InlineData("key\"with\"quotes")]
        [InlineData("it's a \"test\"\\path.with-dashes and spaces")]
        [InlineData("line1\nline2")]
        [InlineData("col1\tcol2")]
        [InlineData("")]
        [InlineData("\r\n")]
        [InlineData("\u2028")]
        [InlineData("\u2029")]
        [InlineData("\u3712")]
        [InlineData("\ud83d\ude00")]
        [InlineData("key \ud83d\udd11 with emoji")]
        public void CanAddDictionaryEntryWithVariousKeys(string key)
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Values = new Dictionary<string, string>
                    {
                        { "Existing", "ExistingValue" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var doc = session.Load<Document>("docs/1");
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Add(key, "AddedValue"));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(2, values.Count);

                Assert.True(values.TryGet(key, out string value), $"Key '{key}' should exist");
                Assert.Equal("AddedValue", value);
                Assert.True(values.TryGet("Existing", out value));
                Assert.Equal("ExistingValue", value);
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [InlineData("Key1")]
        [InlineData("key-with-dash")]
        [InlineData("key with space")]
        [InlineData("key.with.dot")]
        [InlineData("back\\slash")]
        [InlineData("key\"with\"quotes")]
        [InlineData("it's a \"test\"\\path.with-dashes and spaces")]
        [InlineData("line1\nline2")]
        [InlineData("col1\tcol2")]
        [InlineData("\r\n")]
        [InlineData("\u2028")]
        [InlineData("\u2029")]
        [InlineData("\u3712")]
        [InlineData("\ud83d\ude00")]
        [InlineData("key \ud83d\udd11 with emoji")]
        public void CanRemoveDictionaryEntryWithVariousKeys(string key)
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Values = new Dictionary<string, string>
                    {
                        { key, "ToBeRemoved" },
                        { "Surviving", "SurvivingValue" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var doc = session.Load<Document>("docs/1");
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Remove(key));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(1, values.Count);

                Assert.False(values.TryGet(key, out string _), $"Key '{key}' should have been removed");
                Assert.True(values.TryGet("Surviving", out string value));
                Assert.Equal("SurvivingValue", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryViaIndexerWithKeyFromObjectProperty()
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
        public void CanAddDictionaryEntryWithKeyFromObjectProperty()
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

            var keyHolder = new KeyHolder { Key = "Key2" };

            using (var session = store.OpenSession())
            {
                var doc = session.Load<Document>("docs/1");
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Add(keyHolder.Key, "Value2"));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(2, values.Count);

                Assert.True(values.TryGet("Key2", out string value));
                Assert.Equal("Value2", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanRemoveDictionaryEntryWithKeyFromObjectProperty()
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

            var keyHolder = new KeyHolder { Key = "Key1" };

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
                Assert.Equal(1, values.Count);

                Assert.False(values.TryGet("Key1", out string _));
                Assert.True(values.TryGet("Key2", out string value));
                Assert.Equal("Value2", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanAddAndRemoveDictionaryEntriesInSameSession()
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

            using (var session = store.OpenSession())
            {
                var doc = session.Load<Document>("docs/1");
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Add("key-three", "Value3"));
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Remove("Key1"));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(2, values.Count);

                Assert.False(values.TryGet("Key1", out string _));
                Assert.True(values.TryGet("Key2", out string value));
                Assert.Equal("Value2", value);
                Assert.True(values.TryGet("key-three", out value));
                Assert.Equal("Value3", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanAddMultipleDictionaryEntriesWithSpecialKeysInSameSession()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Values = new Dictionary<string, string>()
                }, "docs/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var doc = session.Load<Document>("docs/1");
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Add("key-one", "v1"));
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Add("key.two", "v2"));
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Add("key three", "v3"));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(3, values.Count);

                Assert.True(values.TryGet("key-one", out string value));
                Assert.Equal("v1", value);
                Assert.True(values.TryGet("key.two", out value));
                Assert.Equal("v2", value);
                Assert.True(values.TryGet("key three", out value));
                Assert.Equal("v3", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanAddDictionaryEntryWithKeyValuePairOverload()
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

            using (var session = store.OpenSession())
            {
                var doc = session.Load<Document>("docs/1");
                session.Advanced.Patch(doc, x => x.Values,
                    dict => dict.Add(new KeyValuePair<string, string>("my-special.key", "SpecialValue")));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(2, values.Count);

                Assert.True(values.TryGet("my-special.key", out string value));
                Assert.Equal("SpecialValue", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryWithEnumKeyUsingAdd()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new EnumKeyDocument
                {
                    Checks = new Dictionary<Parts, string>
                    {
                        { Parts.Engine, "Good" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var doc = session.Load<EnumKeyDocument>("docs/1");
                session.Advanced.Patch(doc, x => x.Checks, dict => dict.Add(Parts.Gears, "Bad"));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(EnumKeyDocument.Checks), out BlittableJsonReaderObject values));
                Assert.Equal(2, values.Count);

                Assert.True(values.TryGet("Engine", out string value));
                Assert.Equal("Good", value);
                Assert.True(values.TryGet("Gears", out value));
                Assert.Equal("Bad", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryWithEnumKeyUsingRemove()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new EnumKeyDocument
                {
                    Checks = new Dictionary<Parts, string>
                    {
                        { Parts.Engine, "Good" },
                        { Parts.Gears, "Bad" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var doc = session.Load<EnumKeyDocument>("docs/1");
                session.Advanced.Patch(doc, x => x.Checks, dict => dict.Remove(Parts.Engine));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(EnumKeyDocument.Checks), out BlittableJsonReaderObject values));
                Assert.Equal(1, values.Count);

                Assert.False(values.TryGet("Engine", out string _));
                Assert.True(values.TryGet("Gears", out string value));
                Assert.Equal("Bad", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryWithEnumKeyUsingIndexer()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new EnumKeyDocument
                {
                    Checks = new Dictionary<Parts, string>
                    {
                        { Parts.Engine, "Good" },
                        { Parts.Gears, "Good" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<EnumKeyDocument, string>("docs/1", x => x.Checks[Parts.Engine], "Bad");
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(EnumKeyDocument.Checks), out BlittableJsonReaderObject values));

                Assert.True(values.TryGet("Engine", out string value));
                Assert.Equal("Bad", value);
                Assert.True(values.TryGet("Gears", out value));
                Assert.Equal("Good", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryWithComplexValueAndSpecialCharacterKey()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new NestedDocument
                {
                    Items = new Dictionary<string, NestedValue>()
                }, "docs/1");
                session.SaveChanges();
            }

            var key = "item-one";

            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<NestedDocument, NestedValue>("docs/1",
                    x => x.Items[key], new NestedValue { Name = "Test", Score = 42 });
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(NestedDocument.Items), out BlittableJsonReaderObject items));
                Assert.True(items.TryGet(key, out BlittableJsonReaderObject nested));
                Assert.True(nested.TryGet(nameof(NestedValue.Name), out string name));
                Assert.Equal("Test", name);
                Assert.True(nested.TryGet(nameof(NestedValue.Score), out int score));
                Assert.Equal(42, score);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void PatchDoesNotAffectNonDictionaryProperties()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new DocumentWithMultipleProperties
                {
                    Name = "Original",
                    Count = 10,
                    Values = new Dictionary<string, string>
                    {
                        { "Key1", "Value1" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<DocumentWithMultipleProperties, string>("docs/1", x => x.Name, "Updated");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var doc = session.Load<DocumentWithMultipleProperties>("docs/1");
                Assert.Equal("Updated", doc.Name);
                Assert.Equal(10, doc.Count);
                Assert.Equal("Value1", doc.Values["Key1"]);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void IncrementStillWorksWithPatchPathWrappedConstantSupport()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new DocumentWithMultipleProperties
                {
                    Name = "Test",
                    Count = 10,
                    Values = new Dictionary<string, string>()
                }, "docs/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                session.Advanced.Increment<DocumentWithMultipleProperties, int>("docs/1", x => x.Count, 5);
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var doc = session.Load<DocumentWithMultipleProperties>("docs/1");
                Assert.Equal(15, doc.Count);
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

        private class EnumKeyDocument
        {
            public Dictionary<Parts, string> Checks { get; set; }
        }

        private enum Parts
        {
            Engine = 42,
            Gears = 102,
        }

        private class DocumentWithMultipleProperties
        {
            public string Name { get; set; }
            public int Count { get; set; }
            public Dictionary<string, string> Values { get; set; }
        }

        private class NestedDocument
        {
            public Dictionary<string, NestedValue> Items { get; set; }
        }

        private class NestedValue
        {
            public string Name { get; set; }
            public int Score { get; set; }
        }
    }
}

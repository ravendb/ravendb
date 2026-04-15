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

        // --- Additional tests below ---

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryValueWithIndexerAndKeyContainingQuotes()
        {
            // Ayende's review: test indexer path with key containing embedded quotes
            using var store = GetDocumentStore();

            var keyWithQuotes = "ke\"y1";

            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Values = new Dictionary<string, string>
                    {
                        { keyWithQuotes, "OriginalValue" },
                        { "Key2", "Value2" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            var obj = new { Key = keyWithQuotes };

            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<Document, string>("docs/1", x => x.Values[obj.Key], "UpdatedValue");
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));

                Assert.True(values.TryGet(keyWithQuotes, out string value));
                Assert.Equal("UpdatedValue", value);
                Assert.True(values.TryGet("Key2", out value));
                Assert.Equal("Value2", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryValueWithIndexerAndKeyContainingSpecialCharacters()
        {
            // Test PatchPathWrappedConstantSupport with various special characters in the key
            using var store = GetDocumentStore();

            var specialKeys = new[] { "key-with-dash", "key with space", "key.with.dot", "back\\slash" };

            foreach (var specialKey in specialKeys)
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Document
                    {
                        Values = new Dictionary<string, string>
                        {
                            { specialKey, "OriginalValue" }
                        }
                    }, "docs/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<Document, string>("docs/1", x => x.Values[specialKey], "Updated");
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var doc = commands.Get("docs/1").BlittableJson;
                    Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                    Assert.True(values.TryGet(specialKey, out string value), $"Key '{specialKey}' should exist");
                    Assert.Equal("Updated", value);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanAddDictionaryEntryWithKeyFromVariable()
        {
            // Tests Add path with simple string variable (not object property)
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

            var key = "NewKey";

            using (var session = store.OpenSession())
            {
                var doc = session.Load<Document>("docs/1");
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Add(key, "NewValue"));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(2, values.Count);

                Assert.True(values.TryGet("NewKey", out string value));
                Assert.Equal("NewValue", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanRemoveDictionaryEntryWithKeyFromVariable()
        {
            // Tests Remove path with simple string variable
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
                var doc = session.Load<Document>("docs/1");
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Remove(key));
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
        public void CanRemoveDictionaryEntryWithSpecialCharacterKey()
        {
            using var store = GetDocumentStore();

            var specialKey = "key-with-dash";

            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Values = new Dictionary<string, string>
                    {
                        { specialKey, "Value1" },
                        { "Key2", "Value2" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var doc = session.Load<Document>("docs/1");
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Remove(specialKey));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(1, values.Count);

                Assert.False(values.TryGet(specialKey, out string _));
                Assert.True(values.TryGet("Key2", out string value));
                Assert.Equal("Value2", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanAddAndRemoveDictionaryEntriesInSameSession()
        {
            // Tests patch merging with the new bracket notation
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
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Add("Key3", "Value3"));
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
                Assert.True(values.TryGet("Key3", out value));
                Assert.Equal("Value3", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanAddMultipleDictionaryEntriesWithSpecialKeysInSameSession()
        {
            // Tests patch merging with multiple special-character keys
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
        public void CanOverwriteExistingDictionaryEntryWithSpecialCharacterKey()
        {
            using var store = GetDocumentStore();

            var specialKey = "my-key.with";

            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Values = new Dictionary<string, string>
                    {
                        { specialKey, "OriginalValue" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var doc = session.Load<Document>("docs/1");
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Add(specialKey, "OverwrittenValue"));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(1, values.Count);

                Assert.True(values.TryGet(specialKey, out string value));
                Assert.Equal("OverwrittenValue", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryWithKeyContainingBackslash()
        {
            // Backslash must be escaped first in EscapeForJsString to avoid double-escaping
            using var store = GetDocumentStore();

            var key = "path\\to\\thing";

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
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Add(key, "BackslashValue"));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(2, values.Count);

                Assert.True(values.TryGet(key, out string value));
                Assert.Equal("BackslashValue", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryWithKeyContainingNewlineAndTab()
        {
            using var store = GetDocumentStore();

            var keyWithNewline = "line1\nline2";
            var keyWithTab = "col1\tcol2";

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
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Add(keyWithNewline, "NewlineValue"));
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Add(keyWithTab, "TabValue"));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(2, values.Count);

                Assert.True(values.TryGet(keyWithNewline, out string value));
                Assert.Equal("NewlineValue", value);
                Assert.True(values.TryGet(keyWithTab, out value));
                Assert.Equal("TabValue", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryWithKeyContainingMixedSpecialCharacters()
        {
            // A key that exercises multiple escape rules simultaneously
            using var store = GetDocumentStore();

            var crazyKey = "it's a \"test\"\\path.with-dashes and spaces";

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
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Add(crazyKey, "CrazyValue"));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));

                Assert.True(values.TryGet(crazyKey, out string value));
                Assert.Equal("CrazyValue", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryWithKeyValuePairAndSpecialCharacterKey()
        {
            // Tests the KeyValuePair overload of Add
            using var store = GetDocumentStore();

            var specialKey = "my-special.key";

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
                    dict => dict.Add(new KeyValuePair<string, string>(specialKey, "SpecialValue")));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(2, values.Count);

                Assert.True(values.TryGet(specialKey, out string value));
                Assert.Equal("SpecialValue", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryWithEnumKeyUsingAdd()
        {
            // Regression: enum keys should work with bracket notation
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
            // Regression: enum key Remove should work with bracket notation
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
            // Regression: indexer patch with enum key
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
        public void CanPatchDictionaryWithEmptyStringKey()
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
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Add("", "EmptyKeyValue"));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(1, values.Count);

                Assert.True(values.TryGet("", out string value));
                Assert.Equal("EmptyKeyValue", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void PatchDoesNotAffectNonDictionaryProperties()
        {
            // Regression: adding PatchPathWrappedConstantSupport to path compilation
            // should not break simple property patching
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
            // Regression: Increment uses the same _pathScriptCompilationOptions
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

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryWithLiteralSpecialCharacterKeyInIndexer()
        {
            // Literal string key with special chars in the indexer expression
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Values = new Dictionary<string, string>
                    {
                        { "my-key", "OriginalValue" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<Document, string>("docs/1", x => x.Values["my-key"], "Updated");
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));

                Assert.True(values.TryGet("my-key", out string value));
                Assert.Equal("Updated", value);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void CanPatchDictionaryWithComplexValueAndSpecialCharacterKey()
        {
            // Dictionary with complex value types and special character keys via indexer
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
        public void CanRemoveDictionaryEntryWithQuotesInKey()
        {
            // Remove with a key containing embedded quotes
            using var store = GetDocumentStore();

            var keyWithQuotes = "key\"with\"quotes";

            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Values = new Dictionary<string, string>
                    {
                        { keyWithQuotes, "QuotedValue" },
                        { "NormalKey", "NormalValue" }
                    }
                }, "docs/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var doc = session.Load<Document>("docs/1");
                session.Advanced.Patch(doc, x => x.Values, dict => dict.Remove(keyWithQuotes));
                session.SaveChanges();
            }

            using (var commands = store.Commands())
            {
                var doc = commands.Get("docs/1").BlittableJson;
                Assert.True(doc.TryGet(nameof(Document.Values), out BlittableJsonReaderObject values));
                Assert.Equal(1, values.Count);

                Assert.False(values.TryGet(keyWithQuotes, out string _));
                Assert.True(values.TryGet("NormalKey", out string value));
                Assert.Equal("NormalValue", value);
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

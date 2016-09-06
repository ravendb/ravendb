using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Indexing;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Tests.Core;
using Xunit;

namespace FastTests.Server.Documents.Patching
{
    public class AdvancedPatching : RavenTestBase
    {
        class CustomType
        {
            public string Id { get; set; }
            public string Owner { get; set; }
            public int Value { get; set; }
            public List<string> Comments { get; set; }
            public DateTime Date { get; set; }
            public DateTimeOffset DateOffset { get; set; }
        }

        private readonly CustomType _test = new CustomType
        {
            Id = "someId",
            Owner = "bob",
            Value = 12143,
            Comments = new List<string>(new[] {"one", "two", "seven"})
        };

        //splice(2, 1) will remove 1 elements from position 2 onwards (zero-based)
        string sampleScript = @"
    this.Comments.splice(2, 1);
    this.Id = 'Something new'; 
    this.Value++; 
    this.newValue = ""err!!"";
    this.Comments = this.Comments.Map(function(comment) {   
        return (comment == ""one"") ? comment + "" test"" : comment;
    });";

        [Fact]
        public async Task CanApplyBasicScriptAsPatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(_test);
                    await session.SaveChangesAsync();
                }

                await store.AsyncDatabaseCommands.PatchAsync("someId", new PatchRequest
                {
                    Script = sampleScript
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("someId");
                var result = JsonConvert.DeserializeObject<CustomType>(resultDoc.DataAsJson.ToString());
                Assert.Equal("Something new", result.Id);
                Assert.Equal(2, result.Comments.Count);
                Assert.Equal("one test", result.Comments[0]);
                Assert.Equal("two", result.Comments[1]);
                Assert.Equal(12144, result.Value);
                Assert.Equal("err!!", resultDoc.DataAsJson["newValue"]);
            }
        }

        [Fact]
        public async Task ComplexVariableTest()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.Parse("{\"Email\":null}"), null);

                const string email = "somebody@somewhere.com";
                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.Email = data.Email;",
                    Values =
                    {
                        {"data", new {Email = email}}
                    },
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("doc");
                Assert.Equal(resultDoc.DataAsJson["Email"].Value<string>(), email);
            }
        }

        [Fact]
        public async Task CanUseTrim()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.Parse("{\"Email\":' somebody@somewhere.com '}"), null);

                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.Email = this.Email.trim();",
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("doc");
                Assert.Equal(resultDoc.DataAsJson["Email"].Value<string>(), "somebody@somewhere.com");
            }
        }

        [Fact]
        public async Task CanUseMathFloor()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.Parse("{\"Email\":' somebody@somewhere.com '}"), null);

                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.Age =  Math.floor(1.6);",
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("doc");
                Assert.Equal(resultDoc.DataAsJson["Age"].Value<int>(), 1);
            }
        }

        [Fact]
        public async Task CanUseSplit()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.Parse("{\"Email\":'somebody@somewhere.com'}"), null);

                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.Parts = this.Email.split('@');",
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("doc");
                var parts = resultDoc.DataAsJson["Parts"].Value<RavenJArray>();
                Assert.Equal(parts[0], "somebody");
                Assert.Equal(parts[1], "somewhere.com");
            }
        }

        [Fact]
        public async Task ComplexVariableTest2()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.Parse("{\"Contact\":null}"), null);

                const string email = "somebody@somewhere.com";
                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.Contact = contact.Email;",
                    Values =
                    {
                        {"contact", new {Email = email}}
                    }
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("doc");
                Assert.Equal(resultDoc.DataAsJson["Contact"], email);
            }
        }

        [Fact]
        public async Task CanUseLoDash()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.Parse("{\"Contact\":null}"), null);

                const string email = "somebody@somewhere.com";
                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.Emails = _.times(3, function(i) { return contact.Email + i; });",
                    Values =
                    {
                        {"contact", new {Email = email}}
                    }
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("doc");
                Assert.Equal(new[] {"somebody@somewhere.com0", "somebody@somewhere.com1", "somebody@somewhere.com2"}, resultDoc.DataAsJson.Value<RavenJArray>("Emails").Select(x => x.Value<string>()));
            }
        }

        [Fact]
        public async Task CanPatchUsingRavenJObjectVars()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(_test), null);

                var variable = new {NewComment = "New Comment"};
                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.Comments[0] = variable.NewComment;",
                    Values =
                    {
                        {"variable", RavenJObject.FromObject(variable)}
                    }
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("doc");
                var result = JsonConvert.DeserializeObject<CustomType>(resultDoc.DataAsJson.ToString());
                Assert.Equal(variable.NewComment, result.Comments[0]);
            }
        }

        [Fact]
        public async Task CanRemoveFromCollectionByValue()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(_test), null);

                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.Comments.Remove('two');",
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("doc");
                var result = JsonConvert.DeserializeObject<CustomType>(resultDoc.DataAsJson.ToString());
                Assert.Equal(new[] {"one", "seven"}.ToList(), result.Comments);
            }
        }

        [Fact]
        public async Task CanRemoveFromCollectionByCondition()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(_test), null);

                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.Comments.RemoveWhere(function(el) {return el == 'seven';});",
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("doc");
                var result = JsonConvert.DeserializeObject<CustomType>(resultDoc.DataAsJson.ToString());
                Assert.Equal(new[] {"one", "two"}.ToList(), result.Comments);
            }
        }

        [Fact]
        public async Task CanPatchUsingVars()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(_test), null);

                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.TheName = Name",
                    Values =
                    {
                        {"Name", "ayende"}
                    }
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("doc");
                Assert.Equal("ayende", resultDoc.DataAsJson.Value<string>("TheName"));
            }
        }

        [Fact]
        public async Task CanHandleNonsensePatching()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(_test), null);

                var parseException = await Assert.ThrowsAsync<ErrorResponseException>(async () =>
                {
                    await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                    {
                        Script = "this.Id = 'Something",
                    });
                });

                Assert.Contains("Raven.Server.Documents.Patch.ParseException: Could not parse: " + Environment.NewLine 
                                + "this.Id = 'Something", parseException.Message);
            }
        }

        [Fact]
        public async Task CanThrowIfValueIsWrong()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(_test), null);

                var invalidOperationException = await Assert.ThrowsAsync<ErrorResponseException>(async () =>
                {
                    await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                    {
                        Script = "throw 'problem'",
                    });
                });

                Assert.Contains("System.InvalidOperationException: Unable to execute JavaScript: " + Environment.NewLine
                                + "throw 'problem'" + Environment.NewLine
                                + Environment.NewLine
                                + "Error: " + Environment.NewLine
                                + "problem", invalidOperationException.Message);
            }
        }

        [Fact]
        public async Task CanOutputDebugInformation()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(_test), null);

                var result = await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "output(this.Id)",
                });

                Assert.Equal("someId", result.Value<RavenJArray>("Debug")[0]);
            }
        }

        [Fact]
        public async Task CannotUseInfiniteLoop()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(_test), null);

                var exception = await Assert.ThrowsAsync<ErrorResponseException>(async () =>
                {
                    await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                    {
                        Script = "while(true) {}",
                    });
                });

                Assert.Contains("Unable to execute JavaScript", exception.Message);
                Assert.Contains("The maximum number of statements executed have been reached.", exception.Message);
            }
        }

        [Fact]
        public async Task CanUseToISOString()
        {
            using (var store = GetDocumentStore())
            {
                var date = DateTime.UtcNow;
                var dateOffset = DateTime.Now.AddMilliseconds(100);
                var testObject = new CustomType {Date = date, DateOffset = dateOffset};
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(testObject), null);

                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = @"
this.DateOutput = new Date(this.Date).toISOString();
this.DateOffsetOutput = new Date(this.DateOffset).toISOString();
",
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("doc");
                Assert.Equal(date.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"), resultDoc.DataAsJson.Value<string>("DateOutput"));
                Assert.Equal(dateOffset.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ"), resultDoc.DataAsJson.Value<string>("DateOffsetOutput"));
            }
        }

        [Fact]
        public async Task CanUpdateBasedOnAnotherDocumentProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType {Value = 2});
                    await session.StoreAsync(new CustomType {Value = 1});
                    await session.SaveChangesAsync();
                }

                await store.AsyncDatabaseCommands.PatchAsync("CustomTypes/1", new PatchRequest
                {
                    Script = @"
 var another = LoadDocument(anotherId);
 this.Value = another.Value;
 ",
                    Values =
                    {
                        {"anotherId", "CustomTypes/2"}
                    }
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("CustomTypes/1");
                var result = JsonConvert.DeserializeObject<CustomType>(resultDoc.DataAsJson.ToString());
                Assert.Equal(1, result.Value);
            }
        }

        [Fact]
        public async Task CanPatchMetadata()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType {Value = 2});
                    await session.StoreAsync(new CustomType {Value = 1});
                    await session.SaveChangesAsync();
                }

                await store.AsyncDatabaseCommands.PatchAsync("CustomTypes/1", new PatchRequest
                {
                    Script = @"
 this.Owner = this['@metadata']['Raven-Clr-Type'];
 this['@metadata']['Raven-Python-Type'] = 'Your.CustomType';
 ",
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("CustomTypes/1");
                var result = JsonConvert.DeserializeObject<CustomType>(resultDoc.DataAsJson.ToString());
                Assert.Equal(resultDoc.Metadata["Raven-Clr-Type"], result.Owner);
                Assert.Equal("Your.CustomType", resultDoc.Metadata["Raven-Python-Type"]);
            }
        }

        [Fact]
        public async Task CanUpdateOnMissingProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new {Name = "Ayende"}, "products/1");
                    await session.SaveChangesAsync();
                }

                await store.AsyncDatabaseCommands.PatchAsync("products/1", new PatchRequest
                {
                    Script = "this.Test = 'a';",
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("products/1");
                Assert.Equal("Ayende", resultDoc.DataAsJson.Value<string>("Name"));
                Assert.Equal("a", resultDoc.DataAsJson.Value<string>("Test"));
            }
        }

        [Fact]
        public async Task WillNotErrorOnMissingDocument()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PatchAsync("products/1", new PatchRequest
                {
                    Script = "this.Test = 'a';",
                });
            }
        }

        [Fact]
        public async Task CanCreateDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType {Value = 10});
                    await session.SaveChangesAsync();
                }

                await store.AsyncDatabaseCommands.PatchAsync("CustomTypes/1", new PatchRequest
                {
                    Script = @"PutDocument(
 'NewTypes/1', 
 { 'CopiedValue':  this.Value },
 {'CreatedBy': 'JS_Script'});",
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("NewTypes/1");
                Assert.Equal(10, resultDoc.DataAsJson.Value<int>("CopiedValue"));
                Assert.Equal("JS_Script", resultDoc.Metadata.Value<string>("CreatedBy"));
            }
        }

        [Fact]
        public async Task CanUpdateDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType {Value = 10});
                    await session.SaveChangesAsync();
                }

                await store.AsyncDatabaseCommands.PatchAsync("CustomTypes/1", new PatchRequest
                {
                    Script = @"PutDocument(
 'NewTypes/1', 
 { 'CopiedValue':this.Value },
 {'CreatedBy': 'JS_Script'});

 PutDocument(
 'NewTypes/1', 
 { 'CopiedValue': this.Value },
 {'CreatedBy': 'JS_Script 2'});",
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("NewTypes/1");
                Assert.Equal(10, resultDoc.DataAsJson.Value<int>("CopiedValue"));
                Assert.Equal("JS_Script 2", resultDoc.Metadata.Value<string>("CreatedBy"));
            }
        }

        [Fact]
        public async Task CanCreateMultipleDocuments()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType {Id = "Items/1", Value = 10, Comments = new List<string>(new[] {"one", "two", "three"})});
                    await session.SaveChangesAsync();
                }

                await store.AsyncDatabaseCommands.PatchAsync("Items/1", new PatchRequest
                {
                    Script = @"_.forEach(this.Comments, function(comment){
                                 PutDocument('Comments/', { 'Comment':comment });
                             })",
                });

                var resultDocs = await store.AsyncDatabaseCommands.GetDocumentsAsync(0, 10);
                Assert.Equal(4, resultDocs.Length);

                var docs = await store.AsyncDatabaseCommands.GetAsync(new[] {"Comments/1", "Comments/2", "Comments/3"}, null);
                Assert.Equal("one", docs.Results[0].Value<string>("Comment"));
                Assert.Equal("two", docs.Results[1].Value<string>("Comment"));
                Assert.Equal("three", docs.Results[2].Value<string>("Comment"));
            }
        }

        [Fact]
        public async Task CreateDocumentWillNotThrowIfEmptyKeyProvided()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType { Id = "CustomTypes/1", Value = 10 });
                    await session.SaveChangesAsync();
                }

                await store.AsyncDatabaseCommands.PatchAsync("CustomTypes/1", new PatchRequest
                {
                    Script = @"PutDocument(null, { 'Property': 'Value'});",
                });

                await store.AsyncDatabaseCommands.PatchAsync("CustomTypes/1", new PatchRequest
                {
                    Script = @"PutDocument('    ', { 'Property': 'Value'});",
                });

                Assert.Equal(3, store.DatabaseCommands.GetStatistics().CountOfDocuments);
            }
        }

        [Fact]
        public async Task CreateDocumentShouldThrowInvalidEtagException()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(_test), null);

                var exception = await Assert.ThrowsAsync<ErrorResponseException>(async () =>
                {
                    await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                    {
                        Script = @"PutDocument('Items/1', { Property: 1}, null, 'invalid-etag');",
                    });
                });
                Assert.Contains("Invalid ETag value for document 'Items/1'", exception.Message);
            }
        }

        [Fact]
        public async Task ShouldThrowConcurrencyExceptionIfNonCurrentEtagWasSpecified()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType {Value = 10});
                    await session.SaveChangesAsync();
                }

                var exception = await Assert.ThrowsAsync<ErrorResponseException>(async () =>
                {
                    await store.AsyncDatabaseCommands.PatchAsync("CustomTypes/1", new PatchRequest
                    {
                        Script = @"PutDocument(
 'Items/1', 
 { 'Property':'Value'},
 {}, 123456789 );",
                    });
                });
                Assert.Contains("Document Items/1 does not exists, but Put was called with etag 123456789. Optimistic concurrency violation, transaction will be aborted.", exception.Message);
            }
        }

        [Fact]
        public async Task CanCreateEmptyDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType {Value = 10});
                    await session.SaveChangesAsync();
                }

                await store.AsyncDatabaseCommands.PatchAsync("CustomTypes/1", new PatchRequest
                {
                    Script = @"PutDocument('NewTypes/1', { }, { });",
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("NewTypes/1");
                Assert.Equal(0, resultDoc.DataAsJson.Keys.Count);
            }
        }

        [Fact]
        public async Task CreateDocumentShouldThrowIfSpecifiedJsonIsNullOrEmptyString()
        {
            using (var store = GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(_test), null);

                var exception = await Assert.ThrowsAsync<ErrorResponseException>(async () =>
                {
                    await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                    {
                        Script = @"PutDocument('Items/1', null);",
                    });
                });
                Assert.Contains("Created document must be a valid object which is not null or empty. Document key: 'Items/1'", exception.Message);
            }
        }

        [Fact]
        public async Task CanCreateDocumentsIfPatchingAppliedByIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType
                    {
                        Id = "Item/1",
                        Value = 1
                    });
                    await session.StoreAsync(new CustomType
                    {
                        Id = "Item/2",
                        Value = 2
                    });
                    await session.SaveChangesAsync();
                }

                store.DatabaseCommands.PutIndex("TestIndex", new IndexDefinition
                {
                    Maps = { @"from doc in docs.CustomTypes 
                            select new { doc.Value }" }
                });

                using (var session = store.OpenAsyncSession())
                {
                    await session.Advanced.AsyncDocumentQuery<CustomType>("TestIndex")
                        .WaitForNonStaleResults()
                        .ToListAsync();
                }

                var operation = await store.AsyncDatabaseCommands.UpdateByIndexAsync("TestIndex",
                    new IndexQuery {Query = "Value:1"},
                    new PatchRequest {Script = @"PutDocument('NewItem/3', {'CopiedValue': this.Value });"});
                await operation.WaitForCompletionAsync();

                var documents = await store.AsyncDatabaseCommands.GetDocumentsAsync(0, 10);
                Assert.Equal(3, documents.Length);

                var jsonDocument = store.DatabaseCommands.Get("NewItem/3");
                Assert.Equal(1, jsonDocument.DataAsJson.Value<int>("CopiedValue"));
            }
        }

        [Fact]
        public async Task PreventRecursion()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType
                    {
                        Id = "Item/1",
                        Value = 1
                    });
                    await session.SaveChangesAsync();
                }

                await store.AsyncDatabaseCommands.PatchAsync("Item/1", new PatchRequest
                {
                    Script = "this.Test = this",
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync("Item/1");
                Assert.Equal("1", resultDoc.DataAsJson["Value"]);

                var patchedField = (RavenJObject) resultDoc.DataAsJson["Test"];
                Assert.Equal("1", patchedField["Value"]);

                patchedField = patchedField["Test"] as RavenJObject;
                Assert.Null(patchedField);
            }
        }

        [Fact]
        public async Task CanPerformAdvancedPatching()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(_test);
                    await session.SaveChangesAsync();
                }

                await store.AsyncDatabaseCommands.PatchAsync(_test.Id, new PatchRequest
                {
                    Script = sampleScript,
                });

                var resultDoc = await store.AsyncDatabaseCommands.GetAsync(_test.Id);
                var result = JsonConvert.DeserializeObject<CustomType>(resultDoc.DataAsJson.ToString());

                Assert.NotEqual("Something new", resultDoc.Metadata["@id"]);
                Assert.Equal(2, result.Comments.Count);
                Assert.Equal("one test", result.Comments[0]);
                Assert.Equal("two", result.Comments[1]);
                Assert.Equal(12144, result.Value);
                Assert.Equal("err!!", resultDoc.DataAsJson["newValue"]);
            }
        }

        [Fact]
        public async Task CanPerformAdvancedWithSetBasedUpdates()
        {
            using (var store = GetDocumentStore())
            {
                var item1 = new CustomType
                {
                    Id = "someId/",
                    Owner = "bob",
                    Value = 12143,
                    Comments = new List<string>(new[] { "one", "two", "seven" })
                };
                var item2 = new CustomType
                {
                    Id = "someId/",
                    Owner = "NOT bob",
                    Value = 9999,
                    Comments = new List<string>(new[] { "one", "two", "seven" })
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(item1);
                    await session.StoreAsync(item2);
                    await session.SaveChangesAsync();
                }
                
                store.DatabaseCommands.PutIndex("TestIndex",
                    new IndexDefinition
                    {
                        Maps = { @"from doc in docs.CustomTypes 
                                     select new { doc.Owner }" }
                    });

                WaitForUserToContinueTheTest(store);

                store.OpenSession().Advanced.DocumentQuery<CustomType>("TestIndex")
                    .WaitForNonStaleResults().ToList();

                store.DatabaseCommands.UpdateByIndex("TestIndex",
                    new IndexQuery {Query = "Owner:Bob"},
                    new PatchRequest {Script = sampleScript})
                    .WaitForCompletion();

                var item1ResultJson = store.DatabaseCommands.Get(item1.Id).DataAsJson;
                var item1Result = JsonConvert.DeserializeObject<CustomType>(item1ResultJson.ToString());
                Assert.Equal(2, item1Result.Comments.Count);
                Assert.Equal("one test", item1Result.Comments[0]);
                Assert.Equal("two", item1Result.Comments[1]);
                Assert.Equal(12144, item1Result.Value);
                Assert.Equal("err!!", item1ResultJson["newValue"]);

                var item2ResultJson = store.DatabaseCommands.Get(item2.Id).DataAsJson;
                var item2Result = JsonConvert.DeserializeObject<CustomType>(item2ResultJson.ToString());
                Assert.Equal(9999, item2Result.Value);
                Assert.Equal(3, item2Result.Comments.Count);
                Assert.Equal("one", item2Result.Comments[0]);
                Assert.Equal("two", item2Result.Comments[1]);
                Assert.Equal("seven", item2Result.Comments[2]);
            }
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents.Patching;
//using Raven.Server.Documents.Patch;
using Sparrow.Json;
using Xunit;

namespace FastTests.Server.Documents.Patching
{
    public class AdvancedPatching : RavenTestBase
    {
        private class CustomType
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
            Comments = new List<string>(new[] { "one", "two", "seven" })
        };

        //splice(2, 1) will remove 1 elements from position 2 onwards (zero-based)
        private const string SampleScript = @"
    this.Comments.splice(2, 1);
    this.Owner = 'Something new'; 
    this.Value++; 
    this.newValue = ""err!!"";
    this.Comments = this.Comments.map(function(comment) {   
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

                await store.Operations.SendAsync(new PatchOperation("someId", null, new PatchRequest
                {
                    Script = SampleScript
                }));

                using (var commands = store.Commands())
                {
                    var resultDoc = await commands.GetAsync("someId");
                    var result = commands.Deserialize<CustomType>(resultDoc.BlittableJson);

                    Assert.Equal("Something new", result.Owner);
                    Assert.Equal(2, result.Comments.Count);
                    Assert.Equal("one test", result.Comments[0]);
                    Assert.Equal("two", result.Comments[1]);
                    Assert.Equal(12144, result.Value);
                    Assert.Equal("err!!", resultDoc["newValue"].ToString());
                }
            }
        }

        [Fact]
        public async Task ComplexVariableTest()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, new { Email = (string)null }, null);

                    const string email = "somebody@somewhere.com";
                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "this.Email = data.Email;",
                        Values =
                        {
                            {"data", new { Email = email }}
                        }
                    }));

                    dynamic doc = await commands.GetAsync("doc");
                    string docEmail = doc.Email;

                    Assert.Equal(email, docEmail);
                }
            }
        }

        [Fact]
        public async Task CanUseTrim()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, new { Email = " somebody@somewhere.com " }, null);

                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "this.Email = this.Email.trim();"
                    }));

                    dynamic doc = await commands.GetAsync("doc");
                    string docEmail = doc.Email;

                    Assert.Equal("somebody@somewhere.com", docEmail);
                }
            }
        }

        [Fact]
        public async Task CanUseMathFloor()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, new { Email = " somebody@somewhere.com " }, null);

                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "this.Age = Math.floor(1.6);"
                    }));

                    dynamic doc = await commands.GetAsync("doc");
                    var age = (int)doc.Age;

                    Assert.Equal(1, age);
                }
            }
        }

        [Fact]
        public async Task CanUseSplit()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, new { Email = "somebody@somewhere.com" }, null);

                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "this.Parts = this.Email.split('@');"
                    }));

                    dynamic doc = await commands.GetAsync("doc");
                    string[] parts = doc.Parts;

                    Assert.Equal(parts[0], "somebody");
                    Assert.Equal(parts[1], "somewhere.com");
                }
            }
        }

        [Fact]
        public async Task ComplexVariableTest2()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, new { Contact = (string)null }, null);

                    const string email = "somebody@somewhere.com";
                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "this.Contact = contact.Email;",
                        Values =
                        {
                            {"contact", new {Email = email}}
                        }
                    }));

                    dynamic doc = await commands.GetAsync("doc");
                    var docEmail = doc.Contact.ToString();

                    Assert.Equal(email, docEmail);
                }
            }
        }

        [Fact]
        public async Task CanUseLoDash()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, new { Contact = (string)null }, null);

                    const string email = "somebody@somewhere.com";
                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "this.Emails = _.times(3, function(i) { return contact.Email + i; });",
                        Values =
                        {
                            {"contact", new {Email = email}}
                        }
                    }));

                    dynamic doc = await commands.GetAsync("doc");
                    string[] emails = doc.Emails;

                    Assert.Equal(new[] { "somebody@somewhere.com0", "somebody@somewhere.com1", "somebody@somewhere.com2" }, emails);
                }
            }
        }

        [Fact]
        public async Task CanPatchUsingRavenJObjectVars()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, _test, null);

                    var variable = new { NewComment = "New Comment" };
                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "this.Comments[0] = variable.NewComment;",
                        Values =
                        {
                            {"variable", new { NewComment = "New Comment" }}
                        }
                    }));

                    var doc = await commands.GetAsync("doc");
                    var result = commands.Deserialize<CustomType>(doc.BlittableJson);
                    Assert.Equal(variable.NewComment, result.Comments[0]);
                }
            }
        }

        [Fact]
        public async Task CanRemoveFromCollectionByValue()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, _test, null);

                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "_.pull(this.Comments,'two');"
                    }));

                    var doc = await commands.GetAsync("doc");
                    var result = commands.Deserialize<CustomType>(doc.BlittableJson);
                    Assert.Equal(new[] { "one", "seven" }.ToList(), result.Comments);
                }
            }
        }

        [Fact]
        public async Task CanRemoveFromCollectionByCondition()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, _test, null);

                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "_.remove(this.Comments,function(el) {return el == 'seven';});",
                    }));

                    var doc = await commands.GetAsync("doc");
                    var result = commands.Deserialize<CustomType>(doc.BlittableJson);
                    Assert.Equal(new[] { "one", "two" }.ToList(), result.Comments);
                }
            }
        }

        [Fact]
        public async Task CanPatchUsingVars()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, _test, null);

                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "this.TheName = Name",
                        Values =
                        {
                            {"Name", "ayende"}
                        }
                    }));

                    dynamic doc = await commands.GetAsync("doc");
                    var docName = doc.TheName.ToString();
                    Assert.Equal("ayende", docName);
                }
            }
        }

        [Fact]
        public async Task CanHandleNonsensePatching()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, _test, null);

                    var parseException = await Assert.ThrowsAsync<JavaScriptParseException>(async () =>
                    {
                        await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                        {
                            Script = "this.Id = 'Something",
                        }));
                    });

                    Assert.Contains("Could not parse: " + Environment.NewLine
                                    + "this.Id = 'Something", parseException.Message);
                }
            }
        }

        [Fact]
        public async Task CanThrowIfValueIsWrong()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, _test, null);

                    var invalidOperationException = await Assert.ThrowsAsync<JavaScriptException>(async () =>
                    {
                        await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                        {
                            Script = "throw 'problem'",
                        }));
                    });

                    Assert.Contains("Unable to execute JavaScript: " + Environment.NewLine
                                    + "throw 'problem'" + Environment.NewLine
                                    + Environment.NewLine
                                    + "Error: " + Environment.NewLine
                                    + "problem", invalidOperationException.Message);
                }
            }
        }

        [Fact]
        public async Task CanOutputDebugInformation()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync(_test.Id, null, _test, null);

                    var command = new PatchOperation.PatchCommand(
                        store.Conventions,
                        commands.Context,
                        _test.Id,
                        null,
                        new PatchRequest
                        {
                            Script = "output(__document_id)",
                        },
                        patchIfMissing: null,
                        skipPatchIfChangeVectorMismatch: false,
                        returnDebugInformation: true,
                        test: false);

                    await commands.RequestExecutor.ExecuteAsync(command, commands.Context);

                    var result = command.Result;
                    var array = (BlittableJsonReaderArray)result.Debug["Info"];
                    var someId = array[0].ToString();

                    Assert.Equal(_test.Id, someId);
                }
            }
        }

        [Fact]
        public async Task CannotUseInfiniteLoop()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, _test, null);

                    var exception = await Assert.ThrowsAsync<JavaScriptException>(async () =>
                    {
                        await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                        {
                            Script = "while(true) {}",
                        }));
                    });

                    Assert.Contains("Unable to execute JavaScript", exception.Message);
                    Assert.Contains("The maximum number of statements executed have been reached.", exception.Message);
                }
            }
        }

        [Fact]
        public async Task CanUseToISOString()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    var date = DateTime.UtcNow;
                    var dateOffset = DateTime.Now.AddMilliseconds(100);
                    var testObject = new CustomType { Date = date, DateOffset = dateOffset };

                    await commands.PutAsync("doc", null, testObject, null);

                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = @"
this.DateOutput = new Date(this.Date).toISOString();
this.DateOffsetOutput = new Date(this.DateOffset).toISOString();
",
                    }));

                    var doc = await commands.GetAsync("doc");

                    string dateOutput;
                    doc.BlittableJson.TryGet("DateOutput", out dateOutput);

                    string dateOffsetOutput;
                    doc.BlittableJson.TryGet("DateOffsetOutput", out dateOffsetOutput);

                    Assert.Equal(date.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"), dateOutput);
                    Assert.Equal(dateOffset.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ"), dateOffsetOutput);
                }
            }
        }

        [Fact]
        public async Task CanUpdateBasedOnAnotherDocumentProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType { Value = 2 }, "CustomTypes/1");
                    await session.StoreAsync(new CustomType { Value = 1 }, "CustomTypes/2");
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PatchOperation("CustomTypes/1", null, new PatchRequest
                {
                    Script = @"
var another = LoadDocument(anotherId);
this.Value = another.Value;
",
                    Values =
                    {
                        {"anotherId", "CustomTypes/2"}
                    }
                }));

                using (var commands = store.Commands())
                {
                    var doc = await commands.GetAsync("CustomTypes/1");

                    var result = commands.Deserialize<CustomType>(doc.BlittableJson);
                    Assert.Equal(1, result.Value);
                }
            }
        }

        [Fact]
        public async Task CanPatchMetadata()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType { Value = 2 }, "CustomTypes/1");
                    await session.StoreAsync(new CustomType { Value = 1 }, "CustomTypes/2");
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PatchOperation("CustomTypes/1", null, new PatchRequest
                {
                    Script = @"
        this.Owner = this['@metadata']['Raven-Clr-Type'];
        this['@metadata']['Raven-Python-Type'] = 'Your.CustomType';
        ",
                }));

                using (var commands = store.Commands())
                {
                    dynamic doc = await commands.GetAsync("CustomTypes/1");
                    dynamic metadata = doc[Constants.Documents.Metadata.Key];
                    var clrType = metadata["Raven-Clr-Type"].ToString();
                    var pythonType = metadata["Raven-Python-Type"].ToString();

                    var result = commands.Deserialize<CustomType>(doc.BlittableJson);

                    Assert.Equal(clrType, result.Owner);
                    Assert.Equal("Your.CustomType", pythonType);
                }
            }
        }

        [Fact]
        public async Task CanUpdateOnMissingProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new { Name = "Ayende" }, "products/1");
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PatchOperation("products/1", null, new PatchRequest
                {
                    Script = "this.Test = 'a';",
                }));

                using (var commands = store.Commands())
                {
                    dynamic doc = await commands.GetAsync("products/1");
                    var name = doc.Name.ToString();
                    var test = doc.Test.ToString();

                    Assert.Equal("Ayende", name);
                    Assert.Equal("a", test);
                }
            }
        }


        [Fact]
        public async Task WillNotErrorOnMissingDocument()
        {
            using (var store = GetDocumentStore())
            {
                await store.Operations.SendAsync(new PatchOperation("products/1", null, new PatchRequest
                {
                    Script = "this.Test = 'a';",
                }));
            }
        }

        [Fact]
        public async Task CanCreateDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType { Value = 10 }, "CustomTypes/1");
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PatchOperation("CustomTypes/1", null, new PatchRequest
                {
                    Script = @"PutDocument(
        'NewTypes/1', 
        { 'CopiedValue':  this.Value },
        {'CreatedBy': 'JS_Script'});",
                }));

                using (var commands = store.Commands())
                {
                    dynamic doc = await commands.GetAsync("NewTypes/1");
                    dynamic metadata = doc[Constants.Documents.Metadata.Key];
                    var copiedValue = (int)doc.CopiedValue;
                    var createdBy = metadata.CreatedBy.ToString();

                    Assert.Equal(10, copiedValue);
                    Assert.Equal("JS_Script", createdBy);
                }
            }
        }

        [Fact]
        public async Task CanUpdateDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType { Value = 10 }, "CustomTypes/1");
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PatchOperation("CustomTypes/1", null, new PatchRequest
                {
                    Script = @"PutDocument(
        'NewTypes/1', 
        { 'CopiedValue':this.Value },
        {'CreatedBy': 'JS_Script'});

        PutDocument(
        'NewTypes/1', 
        { 'CopiedValue': this.Value },
        {'CreatedBy': 'JS_Script 2'});",
                }));

                using (var commands = store.Commands())
                {
                    dynamic doc = await commands.GetAsync("NewTypes/1");
                    dynamic metadata = doc[Constants.Documents.Metadata.Key];
                    var copiedValue = (int)doc.CopiedValue;
                    var createdBy = metadata.CreatedBy.ToString();

                    Assert.Equal(10, copiedValue);
                    Assert.Equal("JS_Script 2", createdBy);
                }
            }
        }

        [Fact]
        public async Task CanCreateMultipleDocuments()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType { Id = "Items/1", Value = 10, Comments = new List<string>(new[] { "one", "two", "three" }) });
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PatchOperation("Items/1", null, new PatchRequest
                {
                    Script = @"_.forEach(this.Comments, function(comment){
                                     PutDocument('Comments/' + comment, { 'Comment':comment });
                                 })",
                }));

                using (var commands = store.Commands())
                {
                    var docs = await commands.GetAsync(0, 10);
                    Assert.Equal(4, docs.Length);

                    docs = await commands.GetAsync(new[] { "Comments/one", "Comments/two", "Comments/three" });
                    Assert.Equal("one", docs.ElementAt(0).Comment.ToString());
                    Assert.Equal("two", docs.ElementAt(1).Comment.ToString());
                    Assert.Equal("three", docs.ElementAt(2).Comment.ToString());
                }
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

                await store.Operations.SendAsync(new PatchOperation("CustomTypes/1", null, new PatchRequest
                {
                    Script = @"PutDocument(null, { 'Property': 'Value'});",
                }));

                await store.Operations.SendAsync(new PatchOperation("CustomTypes/1", null, new PatchRequest
                {
                    Script = @"PutDocument('    ', { 'Property': 'Value'});",
                }));

                var stats = await store.Admin.SendAsync(new GetStatisticsOperation());

                Assert.Equal(3, stats.CountOfDocuments);
            }
        }

        [Fact]
        public async Task CreateDocumentShouldThrowInvalidEtagException()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, _test, null);
                }

                var exception = await Assert.ThrowsAsync<JavaScriptException>(async () =>
                {
                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = @"PutDocument('Items/1', { Property: 1}, null, 'invalid-etag');",
                    }));
                });

                Assert.Contains("Document Items/1 does not exist, but Put was called with change vector invalid-etag. Optimistic concurrency violation, transaction will be aborted.", exception.Message);
            }
        }

        [Fact]
        public async Task ShouldThrowConcurrencyExceptionIfNonCurrentEtagWasSpecified()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType { Value = 10 }, "CustomTypes/1");
                    await session.SaveChangesAsync();
                }

                var exception = await Assert.ThrowsAsync<JavaScriptException>(async () =>
                {
                    await store.Operations.SendAsync(new PatchOperation("CustomTypes/1", null, new PatchRequest
                    {
                        Script = @"PutDocument(
    'Items/1', 
    { 'Property':'Value'},
    {}, 123456789 );",
                    }));
                });

                Assert.Contains("Document Items/1 does not exist, but Put was called with change vector 123456789. Optimistic concurrency violation, transaction will be aborted.", exception.Message);
            }
        }

        [Fact]
        public async Task CanCreateEmptyDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType { Value = 10 }, "CustomTypes/1");
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PatchOperation("CustomTypes/1", null, new PatchRequest
                {
                    Script = @"PutDocument('NewTypes/1', { }, { });",
                }));

                using (var commands = store.Commands())
                {
                    var doc = await commands.GetAsync("NewTypes/1");
                    Assert.Equal(0 + 1, doc.BlittableJson.Count); // +1 @metadata
                }
            }
        }

        [Fact]
        public async Task CreateDocumentShouldThrowIfSpecifiedJsonIsNullOrEmptyString()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, _test, null);
                }

                var exception = await Assert.ThrowsAsync<JavaScriptException>(async () =>
                {
                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = @"PutDocument('Items/1', null);",
                    }));
                });
                Assert.Contains("Created document must be a valid object which is not null or empty. Document ID: 'Items/1'", exception.Message);
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

                store.Admin.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Maps = { @"from doc in docs.CustomTypes 
                            select new { doc.Value }" },
                    Name = "TestIndex"
                }}));

                using (var session = store.OpenAsyncSession())
                {
                    await session.Advanced.AsyncDocumentQuery<CustomType>("TestIndex")
                        .WaitForNonStaleResults()
                        .ToListAsync();
                }

                var operation = await store.Operations.SendAsync(new PatchByQueryOperation(
                    new IndexQuery { Query = "FROM INDEX 'TestIndex' WHERE Value = 1" },
                    new PatchRequest { Script = @"PutDocument('NewItem/3', {'CopiedValue': this.Value });" }),
                    CancellationToken.None);
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

                using (var commands = store.Commands())
                {
                    var documents = await commands.GetAsync(0, 10);
                    Assert.Equal(3, documents.Length);

                    dynamic jsonDocument = await commands.GetAsync("NewItem/3");
                    Assert.Equal(1, (int)jsonDocument.CopiedValue);
                }
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

                await store.Operations.SendAsync(new PatchOperation("Item/1", null, new PatchRequest
                {
                    Script = "this.Test = this",
                }));

                using (var commands = store.Commands())
                {
                    dynamic resultDoc = await commands.GetAsync("Item/1");
                    Assert.Equal("1", resultDoc.Value.ToString());

                    var patchedField = resultDoc.Test;
                    Assert.Equal("1", patchedField.Value.ToString());

                    patchedField = patchedField.Test;
                    Assert.True(patchedField == null);
                }
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

                await store.Operations.SendAsync(new PatchOperation(_test.Id, null, new PatchRequest
                {
                    Script = SampleScript,
                }));

                using (var commands = store.Commands())
                {
                    dynamic resultDoc = await commands.GetAsync(_test.Id);
                    var metadata = resultDoc[Constants.Documents.Metadata.Key];
                    var result = commands.Deserialize<CustomType>(resultDoc.BlittableJson);

                    Assert.NotEqual("Something new", metadata[Constants.Documents.Metadata.Id].ToString());
                    Assert.Equal(2, result.Comments.Count);
                    Assert.Equal("one test", result.Comments[0]);
                    Assert.Equal("two", result.Comments[1]);
                    Assert.Equal(12144, result.Value);
                    Assert.Equal("err!!", resultDoc.newValue.ToString());
                }
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

                store.Admin.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Maps = { @"from doc in docs.CustomTypes 
                                     select new { doc.Owner }" },
                        Name = "TestIndex"
                    }}));

                WaitForIndexing(store);

                var operation = store.Operations.Send(new PatchByQueryOperation(
                    new IndexQuery { Query = "FROM INDEX 'TestIndex' WHERE Owner = 'Bob'" },
                    new PatchRequest { Script = SampleScript }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var commands = store.Commands())
                {
                    dynamic item1ResultJson = await commands.GetAsync(item1.Id);
                    var item1Result = commands.Deserialize<CustomType>(item1ResultJson.BlittableJson);

                    Assert.Equal(2, item1Result.Comments.Count);
                    Assert.Equal("one test", item1Result.Comments[0]);
                    Assert.Equal("two", item1Result.Comments[1]);
                    Assert.Equal(12144, item1Result.Value);
                    Assert.Equal("err!!", item1ResultJson.newValue.ToString());

                    dynamic item2ResultJson = await commands.GetAsync(item2.Id);
                    var item2Result = commands.Deserialize<CustomType>(item2ResultJson.BlittableJson);

                    Assert.Equal(9999, item2Result.Value);
                    Assert.Equal(3, item2Result.Comments.Count);
                    Assert.Equal("one", item2Result.Comments[0]);
                    Assert.Equal("two", item2Result.Comments[1]);
                    Assert.Equal("seven", item2Result.Comments[2]);
                }
            }
        }

        [Fact]
        public async Task CanDeserializeModifiedDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, new CustomType { Owner = "somebody@somewhere.com" }, null);

                    var result = await store.Operations.SendAsync(new PatchOperation<CustomType>("doc", null, new PatchRequest
                    {
                        Script = "this.Owner = '123';"
                    }));

                    Assert.Equal(PatchStatus.Patched, result.Status);
                    Assert.Equal("123", result.Document.Owner);

                    result = await store.Operations.SendAsync(new PatchOperation<CustomType>("doc", null, new PatchRequest
                    {
                        Script = "this.Owner = '123';" // not-modified
                    }));

                    Assert.Equal(PatchStatus.NotModified, result.Status);
                    Assert.Equal("123", result.Document.Owner);
                }
            }
        }

        [Fact]
        public void CanDoPatchIfMissing()
        {
            using (var store = GetDocumentStore())
            {
                store.Operations.Send(new PatchOperation("CustomTypes/123", null,
                    new PatchRequest
                    {
                        Script = "{}"
                    }, patchIfMissing: new PatchRequest
                    {
                        Script = "this.Value = 12;"
                    }));

                using (var session = store.OpenSession())
                {
                    var result = session.Load<CustomType>("CustomTypes/123");
                    Assert.NotNull(result);
                    Assert.Equal(12, result.Value);
                }
            }
        }
    }
}

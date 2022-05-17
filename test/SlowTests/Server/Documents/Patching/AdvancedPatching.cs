using Tests.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Patching;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Patching
{
    public class AdvancedPatching : RavenTestBase
    {
        public AdvancedPatching(ITestOutputHelper output) : base(output)
        {
        }

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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanApplyBasicScriptAsPatch(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task ComplexVariableTest(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, new { Email = (string)null }, null);

                    const string email = "somebody@somewhere.com";
                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "this.Email = args.data.Email;",
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanUseTrim(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanUseMathFloor(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, new { Email = " somebody@somewhere.com " }, null);

                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "this.Age = Math.floor(1.6);"
                    }));

                    dynamic doc = await commands.GetAsync("doc");
                    var age = (double)doc.Age;

                    Assert.Equal(1, age);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanUseSplit(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task ComplexVariableTest2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, new { Contact = (string)null }, null);

                    const string email = "somebody@somewhere.com";
                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "this.Contact = args.contact.Email;",
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

      
        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanPatchUsingRavenJObjectVars(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, _test, null);

                    var variable = new { NewComment = "New Comment" };
                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "this.Comments[0] = args.variable.NewComment;",
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanRemoveFromCollectionByValue(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, _test, null);

                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "this.Comments.splice(this.Comments.indexOf('two'),1);",
                    }));

                    var doc = await commands.GetAsync("doc");
                    var result = commands.Deserialize<CustomType>(doc.BlittableJson);
                    Assert.Equal(new[] { "one", "seven" }.ToList(), result.Comments);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanRemoveFromCollectionByCondition(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, _test, null);

                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "this.Comments = this.Comments.filter(function(el) {return el != 'seven';});",
                    }));

                    var doc = await commands.GetAsync("doc");
                    var result = commands.Deserialize<CustomType>(doc.BlittableJson);
                    Assert.Equal(new[] { "one", "two" }.ToList(), result.Comments);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanPatchUsingVars(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, _test, null);

                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = "this.TheName = args.Name",
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanHandleNonsensePatching(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                    Assert.Contains("this.Id = 'Something", parseException.Message);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanThrowIfValueIsWrong(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                    Assert.Contains("problem",invalidOperationException.Message);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanOutputDebugInformation(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync(_test.Id, null, _test, null);

                    var command = new PatchOperation.PatchCommand(
                        commands.Context,
                        _test.Id,
                        null,
                        new PatchRequest
                        {
                            Script = "output(id(this))",
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanOutputNestedDebugInformation(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                CustomType customType;
                using (var session = store.OpenSession())
                {
                    customType = new CustomType
                    {
                        Owner = "Idan",
                        Value = 3

                    };

                    session.Store(customType);
                    session.SaveChanges();
                }

                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new PatchOperation.PatchCommand(
                        context,
                        customType.Id,
                        null,
                        new PatchRequest
                        {
                            Script = "output(this[\"@metadata\"])",
                        },
                        patchIfMissing: null,
                        skipPatchIfChangeVectorMismatch: false,
                        returnDebugInformation: true,
                        test: false);

                    await requestExecutor.ExecuteAsync(command, context);
                    var result = command.Result;
                    var array = (BlittableJsonReaderArray)result.Debug["Info"];
                    var fromPatch = array[0].ToString();

                    Assert.True(fromPatch.Contains("@collection"));

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

                    await Assert.ThrowsAsync<RavenException>(async () =>
                    {
                        await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                        {
                            Script = "while(true) {}",
                        }));
                    });
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanUseToISOString(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanUpdateBasedOnAnotherDocumentProperty(Options options)
        {
            using (var store = GetDocumentStore(options))
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
var another = load(args.anotherId);
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanPatchMetadata(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanUpdateOnMissingProperty(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanCreateDocument(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType { Value = 10 }, "CustomTypes/1");
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PatchOperation("CustomTypes/1", null, new PatchRequest
                {
                    Script = @"put(
        'NewTypes/1', 
        { 'CopiedValue':  this.Value, '@metadata': {'CreatedBy': 'JS_Script'} });",
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanUpdateDocument(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType { Value = 10 }, "CustomTypes/1");
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PatchOperation("CustomTypes/1", null, new PatchRequest
                {
                    Script = @"put(
        'NewTypes/1', 
        { 'CopiedValue':this.Value, '@metadata': {'CreatedBy': 'JS_Script'}} );

        put(
        'NewTypes/1', 
        { 'CopiedValue': this.Value, '@metadata': {'CreatedBy': 'JS_Script 2'} } );",
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanCreateMultipleDocuments(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType { Id = "Items/1", Value = 10, Comments = new List<string>(new[] { "one", "two", "three" }) });
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PatchOperation("Items/1", null, new PatchRequest
                {
                    Script = @"this.Comments.map(function(comment){
                                     put('Comments/' + comment, { 'Comment':comment });
                                 })",
                }));

                using (var commands = store.Commands())
                {
                    var docs = await commands.GetAsync(0, 10);
                    Assert.Equal(4, docs.Count());

                    docs = await commands.GetAsync(new[] { "Comments/one", "Comments/two", "Comments/three" });
                    Assert.Equal("one", docs.ElementAt(0).Comment.ToString());
                    Assert.Equal("two", docs.ElementAt(1).Comment.ToString());
                    Assert.Equal("three", docs.ElementAt(2).Comment.ToString());
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanSkipBeyondCountForLargeIterator(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType { Id = "Items/1", Value = 10, Comments = new List<string>(Enumerable.Range(0, 100).Select(i=>i.ToString()) )});
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PatchOperation("Items/1", null, new PatchRequest
                {
                    Script = @"this.Comments.map(function(comment){
                                     put('Comments/' + comment, { 'Comment':comment });
                                 })",
                }));

                using (var commands = store.Commands())
                {
                    var docs = await commands.GetAsync(101, 10);
                    Assert.Equal(0, docs.Count());
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CreateDocumentWillNotThrowIfEmptyKeyProvided(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType { Id = "CustomTypes/1", Value = 10 });
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PatchOperation("CustomTypes/1", null, new PatchRequest
                {
                    Script = @"put(null, { 'Property': 'Value'});",
                }));

                await store.Operations.SendAsync(new PatchOperation("CustomTypes/1", null, new PatchRequest
                {
                    Script = @"put('    ', { 'Property': 'Value'});",
                }));

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());

                Assert.Equal(3, stats.CountOfDocuments);
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CreateDocumentShouldThrowInvalidEtagException(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, _test, null);
                }

                var exception = await Assert.ThrowsAsync<ConcurrencyException>(async () =>
                {
                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = @"put('Items/1', { Property: 1}, 'invalid-etag');",
                    }));
                });

                Assert.Contains("Document Items/1 does not exist, but Put was called with change vector: invalid-etag. Optimistic concurrency violation, transaction will be aborted.", exception.Message);
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task ShouldThrowConcurrencyExceptionIfNonCurrentEtagWasSpecified(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType { Value = 10 }, "CustomTypes/1");
                    await session.SaveChangesAsync();
                }

                var exception = await Assert.ThrowsAsync<ConcurrencyException>(async () =>
                {
                    await store.Operations.SendAsync(new PatchOperation("CustomTypes/1", null, new PatchRequest
                    {
                        Script = @"put(
    'Items/1', 
    { 'Property':'Value'},
    '123456789' );",
                    }));
                });

                Assert.Contains("Document Items/1 does not exist, but Put was called with change vector: 123456789. Optimistic concurrency violation, transaction will be aborted.", exception.Message);
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanCreateEmptyDocument(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new CustomType { Value = 10 }, "CustomTypes/1");
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PatchOperation("CustomTypes/1", null, new PatchRequest
                {
                    Script = @"put('NewTypes/1', { });",
                }));

                using (var commands = store.Commands())
                {
                    var doc = await commands.GetAsync("NewTypes/1");
                    Assert.Equal(0 + 1, doc.BlittableJson.Count); // +1 @metadata
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CreateDocumentShouldThrowIfSpecifiedJsonIsNullOrEmptyString(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    await commands.PutAsync("doc", null, _test, null);
                }

                var exception = await Assert.ThrowsAsync<RavenException>(async () =>
                {
                    await store.Operations.SendAsync(new PatchOperation("doc", null, new PatchRequest
                    {
                        Script = @"put('Items/1', null);",
                    }));
                });
                Assert.Contains("Created document must be a valid object which is not null or empty. Document ID: 'Items/1'", exception.Message);
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanCreateDocumentsIfPatchingAppliedByIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
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
                    "FROM INDEX 'TestIndex' WHERE Value = 1 update { put('NewItem/3', {'CopiedValue': this.Value });}"
                    ));
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

                using (var commands = store.Commands())
                {
                    var documents = await commands.GetAsync(0, 10);
                    Assert.Equal(3, documents.Count());

                    dynamic jsonDocument = await commands.GetAsync("NewItem/3");
                    Assert.Equal(1, (int)jsonDocument.CopiedValue);
                }
            }
        }

        [Theory]
        [RavenData(true, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        [RavenData(false, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task PreventRecursion(Options options,bool isNative)
        {
            using (var store = GetDocumentStore(options))
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

                var op = new PatchOperation("Item/1", null, new PatchRequest
                {
                    Script = isNative ? 
                        @"
var a = {};
var b = {};
b.a = a;
a.b = b;
this.Else = a;
" : @"
this.Test = this;
",
                });
                
                if (options.JavascriptEngineMode.ToString() == "Jint")
                {
                    try
                    {
                        await store.Operations.SendAsync(op);
                    }
                    catch (Exception e)
                    {
                        var eType = ((object) e).GetType();
                        Assert.Equal(nameof (JavaScriptException), eType.Name);
                        Assert.Contains("Cyclic reference detected", e.Message);
                        return;
                    }
                }
                else
                {
                    await store.Operations.SendAsync(op);
                }

                using (var commands = store.Commands())
                {
                    dynamic resultDoc = await commands.GetAsync("Item/1");
                    Assert.Equal("1", resultDoc.Value<string>("Value"));

                    if (!isNative)
                    {
                        var patchedField = resultDoc.Test;
                        Assert.Equal("1", patchedField.Value.ToString());

                        patchedField = patchedField.Test;
                        Assert.True(patchedField == null);
                    }
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanPerformAdvancedPatching(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanPerformAdvancedWithSetBasedUpdates(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                store.Maintenance.Send(new PutIndexesOperation(new[] {
                    new IndexDefinition
                    {
                        Maps = { @"from doc in docs.CustomTypes 
                                     select new { doc.Owner }" },
                        Name = "TestIndex"
                    }}));

                Indexes.WaitForIndexing(store);

                var operation = store.Operations.Send(new PatchByQueryOperation(
                    $"FROM INDEX \'TestIndex\' WHERE Owner = \'Bob\' UPDATE {{ {SampleScript}}}"));

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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanDeserializeModifiedDocument(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanDoPatchIfMissing(Options options)
        {
            using (var store = GetDocumentStore(options))
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

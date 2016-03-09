using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Jint.Runtime;
using Microsoft.CodeAnalysis.CSharp;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Server.Documents.Patch;
using Raven.Tests.Core;
using Xunit;
using PatchRequest = Raven.Client.Data.PatchRequest;

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
            using (var store = await GetDocumentStore())
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

                var resultJson = await store.AsyncDatabaseCommands.GetAsync("someId");
                var result = JsonConvert.DeserializeObject<CustomType>(resultJson.DataAsJson.ToString());
                Assert.Equal("Something new", result.Id);
                Assert.Equal(2, result.Comments.Count);
                Assert.Equal("one test", result.Comments[0]);
                Assert.Equal("two", result.Comments[1]);
                Assert.Equal(12144, result.Value);
                Assert.Equal("err!!", resultJson.DataAsJson["newValue"]);
            }
        }

        [Fact]
        public async Task ComplexVariableTest()
        {
            using (var store = await GetDocumentStore())
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

                var resultJson = await store.AsyncDatabaseCommands.GetAsync("doc");
                Assert.Equal(resultJson.DataAsJson["Email"].Value<string>(), email);
            }
        }

        [Fact]
        public async Task CanUseTrim()
        {
            using (var store = await GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.Parse("{\"Email\":' somebody@somewhere.com '}"), null);

                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.Email = this.Email.trim();",
                });

                var resultJson = await store.AsyncDatabaseCommands.GetAsync("doc");
                Assert.Equal(resultJson.DataAsJson["Email"].Value<string>(), "somebody@somewhere.com");
            }
        }

         [Fact]
        public async Task CanUseMathFloor()
        {
            using (var store = await GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.Parse("{\"Email\":' somebody@somewhere.com '}"), null);

                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.Age =  Math.floor(1.6);",
                });

                var resultJson = await store.AsyncDatabaseCommands.GetAsync("doc");
                Assert.Equal(resultJson.DataAsJson["Age"].Value<int>(), 1);
            }
        }

        [Fact]
        public async Task CanUseSplit()
        {
            using (var store = await GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.Parse("{\"Email\":'somebody@somewhere.com'}"), null);

                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.Parts = this.Email.split('@');",
                });

                var resultJson = await store.AsyncDatabaseCommands.GetAsync("doc");
                var parts = resultJson.DataAsJson["Parts"].Value<RavenJArray>();
                Assert.Equal(parts[0], "somebody");
                Assert.Equal(parts[1], "somewhere.com");
            }
        }

        [Fact]
         public async Task ComplexVariableTest2()
         {
            using (var store = await GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.Parse("{\"Contact\":null}"), null);

                const string email = "somebody@somewhere.com";
                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.Contact = contact.Email;",
                    Values =
                    {
                        { "contact", new { Email = email } }
                    }
                });

                var resultJson = await store.AsyncDatabaseCommands.GetAsync("doc");
                 Assert.Equal(resultJson.DataAsJson["Contact"], email);
             }
         }

        [Fact]
        public async Task CanUseLoDash()
        {
            using (var store = await GetDocumentStore())
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

                var resultJson = await store.AsyncDatabaseCommands.GetAsync("doc");
                Assert.Equal(new[] {"somebody@somewhere.com0", "somebody@somewhere.com1", "somebody@somewhere.com2"}, resultJson.DataAsJson.Value<RavenJArray>("Emails").Select(x => x.Value<string>()));
            }
        }

        [Fact]
        public async Task CanPatchUsingRavenJObjectVars()
        {
            using (var store = await GetDocumentStore())
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

                var resultJson = await store.AsyncDatabaseCommands.GetAsync("doc");
                var result = JsonConvert.DeserializeObject<CustomType>(resultJson.DataAsJson.ToString());
                Assert.Equal(variable.NewComment, result.Comments[0]);
            }
        }

        [Fact]
        public async Task CanRemoveFromCollectionByValue()
        {
            using (var store = await GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(_test), null);

                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.Comments.Remove('two');",
                });

                var resultJson = await store.AsyncDatabaseCommands.GetAsync("doc");
                var result = JsonConvert.DeserializeObject<CustomType>(resultJson.DataAsJson.ToString());
                Assert.Equal(new[] {"one", "seven"}.ToList(), result.Comments);
            }
        }

        [Fact]
        public async Task CanRemoveFromCollectionByCondition()
        {
            using (var store = await GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(_test), null);

                await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "this.Comments.RemoveWhere(function(el) {return el == 'seven';});",
                });

                var resultJson = await store.AsyncDatabaseCommands.GetAsync("doc");
                var result = JsonConvert.DeserializeObject<CustomType>(resultJson.DataAsJson.ToString());
                Assert.Equal(new[] {"one", "two"}.ToList(), result.Comments);
            }
        }

        [Fact]
        public async Task CanPatchUsingVars()
        {
            using (var store = await GetDocumentStore())
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

                var resultJson = await store.AsyncDatabaseCommands.GetAsync("doc");
                Assert.Equal("ayende", resultJson.DataAsJson.Value<string>("TheName"));
            }
        }

        [Fact]
        public async Task CanHandleNonsensePatching()
        {
            using (var store = await GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(_test), null);

                await Assert.ThrowsAsync<ParseException>(async () =>
                {
                    await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                    {
                        Script = "this.Id = 'Something",
                    });
                });
            }
        }

        [Fact]
        public async Task CanThrowIfValueIsWrong()
        {
            using (var store = await GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(_test), null);

                var invalidOperationException = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                {
                    await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                    {
                        Script = "throw 'problem'",
                    });
                });

                Assert.Contains("problem", invalidOperationException.Message);
            }
        }

        [Fact]
        public async Task CanOutputDebugInformation()
        {
            using (var store = await GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("doc", null, RavenJObject.FromObject(_test), null);

                var result = await store.AsyncDatabaseCommands.PatchAsync("doc", new PatchRequest
                {
                    Script = "output(this.Id)",
                });

                Assert.Equal("someId", result.Value<RavenJArray>("Debug")[0]);
            }
        }
    }
}
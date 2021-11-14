using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests.Server.JavaScript;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_13891 : RavenTestBase
    {
        public RavenDB_13891(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public async Task PatchedDocumentShouldBeReplaced(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                string personId = null;
                using (var s = store.OpenAsyncSession())
                {
                    for (var i = 1; i < 3; i++)
                    {
                        var p = new Person
                        {
                            Modifications = new List<Person.UserChange>
                            {
                                new Person.UserChange
                                {
                                    Id = $"{i}",
                                    Timestamp = DateTime.Now
                                }
                            }
                        };
                        await s.StoreAsync(p);
                        personId = p.Id;
                    }

                    await s.SaveChangesAsync();

                    s.Advanced.Defer(
                        new PatchCommandData(
                            personId,
                            null,
                            new PatchRequest
                            {
                                Script = $@"this.Text = 'aaa';"
                            },
                            null));

                    var person1 = await s.LoadAsync<Person>(personId);
                    Assert.True(string.IsNullOrEmpty(person1.Text));
                    Assert.Equal(1, person1.Modifications.Count);
                    await s.SaveChangesAsync();

                    var person2 = await s.LoadAsync<Person>(personId);
                    Assert.Equal(person1, person2);
                    Assert.Equal("aaa", person2.Text);
                    Assert.Equal(1, person2.Modifications.Count);
                }
            }
        }

        private class Person
        {
            public string Id { get; set; }
            public string Text { get; set; }
            public List<UserChange> Modifications { get; set; }

            public class UserChange
            {
                public string Id { get; set; }
                public DateTime Timestamp { get; set; }
            }
        }
    }
}

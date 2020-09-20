using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using FastTests;
using Raven.Client.Documents.Commands.MultiGet;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.MultiGet
{
    public class MultiGetBasic : RavenTestBase
    {
        public MultiGetBasic(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
            public string Info { get; set; }
            public bool Active { get; set; }
            public DateTime Created { get; set; }

            public User()
            {
                Name = string.Empty;
                Age = default(int);
                Info = string.Empty;
                Active = false;
            }
        }

        [Fact]
        public void CanUseMultiGetToBatchGetDocumentRequests()
        {
            using (var store = GetDocumentStore())
            {
                const string docs = "/docs";
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Ayende" });
                    session.Store(new User { Name = "Oren" });
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var command = new MultiGetCommand(commands.RequestExecutor, new List<GetRequest>
                    {
                        new GetRequest
                        {
                            Url = docs,
                            Query = "?id=users/1-A"
                        },
                        new GetRequest
                        {
                            Url = docs,
                            Query = "?id=users/2-A"
                        }
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    string name;
                    var result = (BlittableJsonReaderObject)command.Result[0].Result;
                    var results = (BlittableJsonReaderArray)result["Results"];
                    result = (BlittableJsonReaderObject)results[0];
                    Assert.True(result.TryGet("Name", out name));
                    Assert.Equal("Ayende", name);

                    result = (BlittableJsonReaderObject)command.Result[1].Result;
                    results = (BlittableJsonReaderArray)result["Results"];
                    result = (BlittableJsonReaderObject)results[0];
                    Assert.True(result.TryGet("Name", out name));
                    Assert.Equal("Oren", name);
                }
            }
        }

        [Fact]
        public void CanUseMultiQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Ayende" });
                    session.Store(new User { Name = "Oren" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(u => u.Name == "Ayende")
                        .ToArray();
                }

                using (var commands = store.Commands())
                {
                    var command = new MultiGetCommand(commands.RequestExecutor, new List<GetRequest>
                    {
                        new GetRequest
                        {
                            Url = "/queries",
                            Query = "?query=FROM Users WHERE Name = 'Ayende'"
                        },
                        new GetRequest
                        {
                            Url = "/queries",
                            Query = "?query=FROM Users WHERE Name = 'Oren'"
                        }
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    string name;
                    var result = (BlittableJsonReaderObject)command.Result[0].Result;
                    var results = (BlittableJsonReaderArray)result["Results"];
                    result = (BlittableJsonReaderObject)results[0];
                    Assert.True(result.TryGet("Name", out name));
                    Assert.Equal("Ayende", name);

                    result = (BlittableJsonReaderObject)command.Result[1].Result;
                    results = (BlittableJsonReaderArray)result["Results"];
                    result = (BlittableJsonReaderObject)results[0];
                    Assert.True(result.TryGet("Name", out name));
                    Assert.Equal("Oren", name);
                }
            }
        }

        [Fact]
        public void CanHandleCaching()
        {
            using (var store = GetDocumentStore())
            {
                const string docs = "/docs";
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Ayende" });
                    session.Store(new User { Name = "Oren" });
                    session.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var command = new MultiGetCommand(commands.RequestExecutor, new List<GetRequest>
                    {
                        new GetRequest
                        {
                            Url = docs,
                            Query = "?id=users/1-A"
                        },
                        new GetRequest
                        {
                            Url = docs,
                            Query = "?id=users/2-A"
                        }
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    Assert.True(command.Result[0].Headers.ContainsKey("ETag"));
                    Assert.True(command.Result[1].Headers.ContainsKey("ETag"));

                    command = new MultiGetCommand(commands.RequestExecutor, new List<GetRequest>
                    {
                        new GetRequest
                        {
                            Url = docs,
                            Query = "?id=users/1-A",
                            Headers =
                            {
                                {"If-None-Match", command.Result[0].Headers["ETag"]}
                            }
                        },
                        new GetRequest
                        {
                            Url = docs,
                            Query = "?id=users/2-A",
                            Headers =
                            {
                                {"If-None-Match", command.Result[1].Headers["ETag"]}
                            }
                        }
                    });

                    commands.RequestExecutor.Execute(command, commands.Context);

                    Assert.Equal(HttpStatusCode.NotModified, command.Result[0].StatusCode);
                    Assert.Equal(HttpStatusCode.NotModified, command.Result[1].StatusCode);
                }
            }
        }
    }
}

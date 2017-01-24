using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using FastTests;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Data;
using Raven.Imports.Newtonsoft.Json;

using Xunit;

namespace SlowTests.Tests.MultiGet
{
    public class MultiGetBasic : RavenTestBase
    {
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
        public async Task CanUseMultiGetToBatchGetDocumentRequests()
        {
            using (var store = GetDocumentStore())
            {
                var docs = $"/databases/{store.DefaultDatabase}/docs";
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Ayende" });
                    session.Store(new User { Name = "Oren" });
                    session.SaveChanges();
                }

                var request = (HttpWebRequest)WebRequest.Create(store.Url.ForDatabase(store.DefaultDatabase) + "/multi_get");
                request.Method = "POST";
                using (var stream = await request.GetRequestStreamAsync())
                {
                    var streamWriter = new StreamWriter(stream);
                    JsonExtensions.CreateDefaultJsonSerializer().Serialize(streamWriter, new[]
                    {
                        new GetRequest
                        {
                            Url = docs,
                            Query = "?id=users/1"
                        },
                        new GetRequest
                        {
                            Url = docs,
                            Query = "?id=users/2"
                        },
                    });
                    streamWriter.Flush();
                    stream.Flush();
                }

                using (var resp = await request.GetResponseAsync())
                using (var stream = resp.GetResponseStream())
                {
                    var result = new StreamReader(stream).ReadToEnd();
                    Assert.Contains("Ayende", result);
                    Assert.Contains("Oren", result);
                }
            }
        }

        [Fact]
        public async Task CanUseMultiQuery()
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


                var request = (HttpWebRequest)WebRequest.Create(store.Url.ForDatabase(store.DefaultDatabase) + "/multi_get");
                request.Method = "POST";
                using (var stream = await request.GetRequestStreamAsync())
                {
                    var streamWriter = new StreamWriter(stream);
                    JsonExtensions.CreateDefaultJsonSerializer().Serialize(streamWriter, new[]
                    {
                        new GetRequest
                        {
                            Url = "/indexes/dynamic/Users",
                            Query = "query=Name:Ayende"
                        },
                        new GetRequest
                        {
                            Url = "/indexes/dynamic/Users",
                            Query = "query=Name:Oren"
                        },
                    });
                    streamWriter.Flush();
                    stream.Flush();
                }

                using (var resp = await request.GetResponseAsync())
                using (var stream = resp.GetResponseStream())
                {
                    var result = new StreamReader(stream).ReadToEnd();
                    Assert.Contains("Ayende", result);
                    Assert.Contains("Oren", result);
                }
            }
        }

        public class Results
        {
            public GetResponse[] results;
        }

        [Fact]
        public async Task CanHandleCaching()
        {
            using (var store = GetDocumentStore())
            {
                var docs = $"/databases/{store.DefaultDatabase}/docs";
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Ayende" });
                    session.Store(new User { Name = "Oren" });
                    session.SaveChanges();
                }

                var request = (HttpWebRequest)WebRequest.Create(store.Url.ForDatabase(store.DefaultDatabase) + "/multi_get");
                request.Method = "POST";
                using (var stream = await request.GetRequestStreamAsync())
                {
                    var streamWriter = new StreamWriter(stream);
                    JsonExtensions.CreateDefaultJsonSerializer().Serialize(streamWriter, new[]
                    {
                        new GetRequest
                        {
                            Url = docs,
                            Query = "?id=users/1"
                        },
                        new GetRequest
                        {
                            Url = docs,
                            Query = "?id=users/2"
                        },
                    });
                    streamWriter.Flush();
                    stream.Flush();
                }

                Results results;
                using (var resp = await request.GetResponseAsync())
                using (var stream = resp.GetResponseStream())
                {
                    var result = new StreamReader(stream).ReadToEnd();
                    results = JsonConvert.DeserializeObject<Results>(result, Default.Converters);
                    Assert.True(results.results[0].Headers.ContainsKey("ETag"));
                    Assert.True(results.results[1].Headers.ContainsKey("ETag"));
                }

                request = (HttpWebRequest)WebRequest.Create(store.Url.ForDatabase(store.DefaultDatabase) + "/multi_get");
                request.Method = "POST";
                using (var stream = await request.GetRequestStreamAsync())
                {
                    var streamWriter = new StreamWriter(stream);
                    JsonExtensions.CreateDefaultJsonSerializer().Serialize(streamWriter, new[]
                    {
                        new GetRequest
                        {
                            Url = docs,
                            Query = "?id=users/1",
                            Headers =
                            {
                                {"If-None-Match", results.results[0].Headers["ETag"]}
                            }
                        },
                        new GetRequest
                        {
                            Url = docs,
                            Query = "?id=users/2",
                            Headers =
                            {
                                {"If-None-Match", results.results[1].Headers["ETag"]}
                            }
                        },
                    });
                    streamWriter.Flush();
                    stream.Flush();
                }

                using (var resp = await request.GetResponseAsync())
                using (var stream = resp.GetResponseStream())
                {
                    var result = new StreamReader(stream).ReadToEnd();
                    results = JsonConvert.DeserializeObject<Results>(result, Default.Converters);
                    Assert.Equal(304, results.results[0].StatusCode);
                    Assert.Equal(304, results.results[1].StatusCode);
                }
            }
        }
    }
}

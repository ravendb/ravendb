using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class LoadIntoStreamAsync : RavenTestBase
    {
        public LoadIntoStreamAsync(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanLoadByIdsIntoStreamAsync(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                InsertData(store);

                using (var stream = new MemoryStream())
                using (var session = store.OpenAsyncSession())
                {
                    var ids = new List<string> { "employees/1-A", "employees/4-A", "employees/7-A" };
                    await session.Advanced.LoadIntoStreamAsync(ids, stream);

                    stream.Position = 0;
                    var json = JObject.Load(new JsonTextReader(new StreamReader(stream)));
                    var res = json.GetValue("Results");

                    Assert.Equal(res.Children().Count(), 3);

                    var names = new List<string> { "Aviv", "Maxim", "Michael" };
                    foreach (var v in res)
                    {
                        var name = v["FirstName"].ToString();
                        Assert.True(names.Contains(name));
                        names.Remove(name);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanLoadStartingWithIntoStreamAsync(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                InsertData(store);

                var names = new List<string> { "Aviv", "Iftah", "Tal", "Maxim", "Karmel", "Grisha", "Michael" };

                using (var stream = new MemoryStream())
                using (var session = store.OpenAsyncSession())
                {
                    await session.Advanced.LoadStartingWithIntoStreamAsync("employees/", stream);

                    stream.Position = 0;
                    var json = JObject.Load(new JsonTextReader(new StreamReader(stream)));
                    var res = json.GetValue("Results");

                    Assert.Equal(res.Children().Count(), 7);

                    foreach (var v in res)
                    {
                        var name = v["FirstName"].ToString();
                        Assert.True(names.Contains(name));
                        names.Remove(name);
                    }
                }
            }
        }
        private static void InsertData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { FirstName = "Aviv" });
                session.Store(new Employee { FirstName = "Iftah" });
                session.Store(new Employee { FirstName = "Tal" });
                session.Store(new Employee { FirstName = "Maxim" });
                session.Store(new Employee { FirstName = "Karmel" });
                session.Store(new Employee { FirstName = "Grisha" });
                session.Store(new Employee { FirstName = "Michael" });
                session.SaveChanges();
            }
        }
        private class Employee
        {
            public string FirstName { get; set; }
        }

    }
}




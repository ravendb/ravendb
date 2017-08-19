using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Xunit;

namespace FastTests.Client
{
    public class LoadIntoStream : RavenTestBase
    {
        [Fact]
        public void CanLoadByIdsIntoStream()
        {
            using (var store = GetDocumentStore())
            {
                InsertData(store);

                using (var stream = new MemoryStream())
                using (var session = store.OpenSession())
                {
                    var ids = new List<string> { "employees/1-A", "employees/4-A", "employees/7-A" };
                    session.Advanced.LoadIntoStream(ids, stream);

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

        [Fact]
        public void CanLoadStartingWithIntoStream()
        {
            using (var store = GetDocumentStore())
            {
                InsertData(store);

                using (var stream = new MemoryStream())
                using (var session = store.OpenSession())
                {
                    session.Advanced.LoadStartingWithIntoStream("employees/", stream);
                    stream.Position = 0;
                    var json = JObject.Load(new JsonTextReader(new StreamReader(stream)));
                    var res = json.GetValue("Results");
                    Assert.Equal(res.Children().Count(), 7);

                    var names = new List<string> { "Aviv", "Iftah", "Tal", "Maxim", "Karmel", "Grisha", "Michael" };
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




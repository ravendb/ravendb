using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class QueryIntoStream : RavenTestBase
    {
        public QueryIntoStream(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void QueryWithToStream()
        {
            using (var store = GetDocumentStore())
            {
                InsertData(store);
            
                using (var stream = new MemoryStream())
                using (var session = store.OpenSession())
                {
                    session.Query<Employee, EmployeeIndex>()                       
                           .Where(x => x.FirstName == "Robert")
                           .ToStream(stream);

                    stream.Position = 0;
                    var json = JObject.Load(new JsonTextReader(new StreamReader(stream)));
                    var res = json.GetValue("Results");

                    Assert.Equal(res.Children().Count(), 3);

                    foreach (var v in res)                   
                        Assert.Equal(v["FirstName"], "Robert");                   
                }
            }
        }

        [Fact]
        public void DocumentQueryWithToStream()
        {
            using (var store = GetDocumentStore())
            {
                InsertData(store);

                using (var stream = new MemoryStream())
                using (var session = store.OpenSession())
                {
                    session.Advanced.DocumentQuery<Employee, EmployeeIndex>()
                           .WhereEquals(x => x.FirstName, "Robert")
                           .ToStream(stream);

                    stream.Position = 0;
                    var json = JObject.Load(new JsonTextReader(new StreamReader(stream)));
                    var res = json.GetValue("Results");

                    Assert.Equal(res.Children().Count(), 3);

                    foreach (var v in res)
                        Assert.Equal(v["FirstName"], "Robert");
                }
            }
        }

        [Fact]
        public async Task QueryWithToStreamAsync()
        {
            using (var store = GetDocumentStore())
            {
                InsertData(store);

                using (var stream = new MemoryStream())
                using (var session = store.OpenAsyncSession())
                {
                    await session.Query<Employee, EmployeeIndex>()
                           .Where(x => x.FirstName == "Robert")
                           .ToStreamAsync(stream);

                    stream.Position = 0;
                    var json = JObject.Load(new JsonTextReader(new StreamReader(stream)));
                    var res = json.GetValue("Results");

                    Assert.Equal(res.Children().Count(), 3);

                    foreach (var v in res)
                        Assert.Equal(v["FirstName"], "Robert");
                }
            }
        }

        [Fact]
        public async Task DocumentQueryWithToStreamAsync()
        {
            using (var store = GetDocumentStore())
            {
                InsertData(store);

                using (var stream = new MemoryStream())
                using (var session = store.OpenAsyncSession())
                {
                    await session.Advanced.AsyncDocumentQuery<Employee, EmployeeIndex>()
                        .WhereEquals(x => x.FirstName, "Robert")
                        .ToStreamAsync(stream);

                    stream.Position = 0;
                    var json = JObject.Load(new JsonTextReader(new StreamReader(stream)));
                    var res = json.GetValue("Results");

                    Assert.Equal(res.Children().Count(), 3);

                    foreach (var v in res)
                        Assert.Equal(v["FirstName"], "Robert");
                }
            }
        }

        private class Employee
        {
            public string FirstName { get; set; }
        }

        private class EmployeeIndex : AbstractIndexCreationTask<Employee>
        {
            public EmployeeIndex()
            {
                Map = docs => from doc in docs select new { doc.FirstName };            
            }
        }

        private void InsertData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { FirstName = "Aviv" });
                session.Store(new Employee { FirstName = "Robert" });
                session.Store(new Employee { FirstName = "Robert" });
                session.Store(new Employee { FirstName = "Maxim" });
                session.Store(new Employee { FirstName = "Karmel" });
                session.Store(new Employee { FirstName = "Grisha" });
                session.Store(new Employee { FirstName = "Robert" });
                session.SaveChanges();
            }
            new EmployeeIndex().Execute(store);
            Indexes.WaitForIndexing(store);
        }
    }
}

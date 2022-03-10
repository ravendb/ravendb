using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Documents.Indexing;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class QueryIntoStream : RavenTestBase
    {
        public QueryIntoStream(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData]
        public void QueryWithToStream(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData]
        public void DocumentQueryWithToStream(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData]
        public async Task QueryWithToStreamAsync(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData]
        public async Task DocumentQueryWithToStreamAsync(Options options)
        {
            using (var store = GetDocumentStore(options))
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

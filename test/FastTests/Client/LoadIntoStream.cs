using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Transformers;
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
        public void CanLoadByIdsIntoStreamUsingTransformer()
        {
            using (var store = GetDocumentStore())
            {
                new TestDocumentTransformer().Execute(store);
                InsertTestDocuments(store);

                using (var stream = new MemoryStream())
                using (var session = store.OpenSession())
                {
                    var ids = new[] { "TestDocuments/1-A", "TestDocuments/2-A" };
                    session.Advanced.LoadIntoStream<TestDocumentTransformer>(ids, stream);

                    stream.Position = 0;
                    var json = JObject.Load(new JsonTextReader(new StreamReader(stream)));
                    var res = json.GetValue("Results");

                    Assert.Equal(2, res.Children().Count());
                    Assert.Equal(100, res.First["$values"].First["Val"]);
                    Assert.Equal(200, res.Last["$values"].First["Val"]);
                }
            }
        }

        [Fact]
        public void CanLoadByIdsIntoStreamUsingTransformerName()
        {
            using (var store = GetDocumentStore())
            {
                var transformer = new TestDocumentTransformer();
                transformer.Execute(store);
                InsertTestDocuments(store);

                using (var stream = new MemoryStream())
                using (var session = store.OpenSession())
                {
                    var ids = new[] { "TestDocuments/1-A", "TestDocuments/2-A" };
                    session.Advanced.LoadIntoStream(ids, transformer.TransformerName, stream);

                    stream.Position = 0;
                    var json = JObject.Load(new JsonTextReader(new StreamReader(stream)));
                    var res = json.GetValue("Results");

                    Assert.Equal(2, res.Children().Count());
                    Assert.Equal(100, res.First["$values"].First["Val"]);
                    Assert.Equal(200, res.Last["$values"].First["Val"]);
                }
            }
        }

        [Fact]
        public void CanLoadByIdsIntoStreamUsingTransformerType()
        {
            using (var store = GetDocumentStore())
            {
                var transformer = new TestDocumentTransformer();
                transformer.Execute(store);
                InsertTestDocuments(store);

                using (var stream = new MemoryStream())
                using (var session = store.OpenSession())
                {
                    var ids = new[] { "TestDocuments/1-A", "TestDocuments/2-A" };
                    session.Advanced.LoadIntoStream(ids, transformer.GetType(), stream);

                    stream.Position = 0;
                    var json = JObject.Load(new JsonTextReader(new StreamReader(stream)));
                    var res = json.GetValue("Results");

                    Assert.Equal(2, res.Children().Count());
                    Assert.Equal(100, res.First["$values"].First["Val"]);
                    Assert.Equal(200, res.Last["$values"].First["Val"]);
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

        [Fact]
        public void CanLoadStartingWithIntoStreamUsingTransformer()
        {
            using (var store = GetDocumentStore())
            {
                new TestDocumentTransformer().Execute(store);
                InsertTestDocuments(store);

                using (var stream = new MemoryStream())
                using (var session = store.OpenSession())
                {
                    session.Advanced.LoadStartingWithIntoStream<TestDocumentTransformer>("TestDocuments/", stream);

                    stream.Position = 0;
                    var json = JObject.Load(new JsonTextReader(new StreamReader(stream)));
                    var res = json.GetValue("Results");

                    Assert.Equal(2, res.Children().Count());
                    Assert.Equal(100, res.First["$values"].First["Val"]);
                    Assert.Equal(200, res.Last["$values"].First["Val"]);
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

        private static void InsertTestDocuments(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new TestDocument { Value = 1 });
                session.Store(new TestDocument { Value = 2 });
                session.SaveChanges();
            }
        }

        private class Employee
        {
            public string FirstName { get; set; }
        }

        private class TestDocument
        {
            public int Value { get; set; }
        }

        private class TestDocumentTransformer : AbstractTransformerCreationTask<TestDocument>
        {
            public class Output
            {
                public int Val { get; set; }
            }

            public TestDocumentTransformer()
            {
                TransformResults = results =>
                    from result in results
                    select new Output
                    {
                        Val = result.Value * 100
                    };
            }
        }
    }
}




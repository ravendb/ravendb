using Raven.Client;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10594 : RavenTestBase
    {
        public RavenDB_10594(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name;
        }

        private class MyIndex : AbstractIndexCreationTask<User>
        {
            public MyIndex()
            {
                Map = users => from u in users
                               select new
                               {
                                   _ = CreateField("AName", u.Name),
                                   u.Name
                               };
                Index(Constants.Documents.Indexing.Fields.AllFields, FieldIndexing.Search);
                Analyze(Constants.Documents.Indexing.Fields.AllFields,
                    "StandardAnalyzer");
            }
        }

        [Fact]
        public void CanUseDefaultFieldToSetAnalyzer()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User{ Name = "Oren Eini" });
                    session.SaveChanges();
                }
                new MyIndex().Execute(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var users = session.Advanced.DocumentQuery<User, MyIndex>()
                        .WhereEquals("AName", "Oren")
                        .ToList();

                    Assert.Equal(1, users.Count);
                }
            }
        }

    }
}

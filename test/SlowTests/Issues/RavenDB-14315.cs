using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14315 : RavenTestBase
    {
        public RavenDB_14315(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanEnumerateOnLsv()
        {
            using var store = GetDocumentStore();
            store.ExecuteIndex(new User_Index());
            var u = new User
            {
                Items = new Dictionary<string, string> { { "k", "v" }, { "o", "p" }, { "Name", "E" } },
                Name = "E",
                Arr = new[] { "E", "G" },
                LongString = new string('a', 10_000)
            };

            using (var session = store.OpenSession())
            {
                session.Store(u);

                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var results = session.Query<User_Index.Result, User_Index>().ProjectInto<User_Index.Result>().First();

                Assert.Equal(u.Items["Name"].First(), results.NameDictionary);
                Assert.Equal(u.Name.First(), results.Name);
                Assert.Equal(u.Arr.First().First(), results.NameArray);
                Assert.Equal(u.LongString.First(), results.LongString);
            }
        }

        private class User
        {
            public Dictionary<string, string> Items;
            public string Name { get; set; }
            public string[] Arr { get; set; }
            public string LongString { get; set; }
        }

        private class User_Index : AbstractIndexCreationTask<User, User_Index.Result>
        {
            internal class Result
            {
                public char NameDictionary { get; set; }
                public char Name { get; set; }
                public char NameArray { get; set; }
                public char LongString { get; set; }
            }

            public User_Index()
            {
                Map = users => from user in users
                               select new Result
                               {
                                   NameDictionary = user.Items["Name"].First(),
                                   Name = user.Name.First(),
                                   NameArray = user.Arr.First().First(),
                                   LongString = user.LongString.First()
                               };

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}

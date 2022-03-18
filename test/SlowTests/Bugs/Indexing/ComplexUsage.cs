using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Indexing
{
    public class ComplexUsage : RavenTestBase
    {
        public ComplexUsage(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldNotOutputNull()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Account
                    {
                        Id = "accounts/2",
                        Name = null
                    });
                    session.Store(new Account
                    {
                        Id = "accounts/1",
                        Name = "Hibernating Rhinos"
                    });
                    session.Store(new Design()
                    {
                        Id = "designs/1",
                        Name = "Design 1",
                        AccountId = "accounts/1"
                    });
                    session.Store(new User()
                    {
                        Id = "users/1",
                        Name = null,
                        AccountId = "accounts/1"
                    });
                    session.Store(new User()
                    {
                        Id = "users/2",
                        Name = "User 1",
                        AccountId = "accounts/1"
                    });
                    session.SaveChanges();
                }

                new Accounts_Search().Execute(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var objects = session.Query<object, Accounts_Search>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ProjectInto<AccountIndex>()
                        .OrderBy(x => x.AccountId) //this is just to make sure the second result is last for the test
                        .ToArray();


                    Assert.Equal("Hibernating Rhinos", objects[0].AccountName);

                    //Ayende, the account name for the second item
                    //should be null but it's actually the string
                    //NULL_VALUE.
                    Assert.Null(objects[1].AccountName);
                }
            }
        }

        private class AccountIndex
        {
            public string AccountId { get; set; }
            public string AccountName { get; set; }
            public string[] UserName { get; set; }
            public string[] DesignName { get; set; }
        }

        private class Account
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class User
        {
            public string Id { get; set; }
            public string AccountId { get; set; }
            public string Name { get; set; }
        }

        private class Design
        {
            public string Id { get; set; }
            public string AccountId { get; set; }
            public string Name { get; set; }
        }

        private class Accounts_Search : AbstractIndexCreationTask<object, Account>
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                var fieldOptions1 = new IndexFieldOptions { Indexing = FieldIndexing.Exact, Storage = FieldStorage.Yes };
                var fieldOptions2 = new IndexFieldOptions { Indexing = FieldIndexing.Search, Storage = FieldStorage.Yes };

                var index = new IndexDefinition
                {
                    Name = this.IndexName,

                    Maps =
                    {
                        @"
                        from doc in docs.WhereEntityIs(""Accounts"", ""Users"", ""Designs"")
                        let acc = doc[""@metadata""][""@collection""] == ""Accounts"" ? doc : null
                        let user = doc[""@metadata""][""@collection""] == ""Users"" ? doc : null
                        let design = doc[""@metadata""][""@collection""] == ""Designs"" ? doc : null
                        select new 
                        {
                            AccountId = acc != null ? acc.Id : (user != null ? user.AccountId : design.AccountId),
                            AccountName = acc != null ? acc.Name : null,
                            UserName = user != null ? user.Name : null,
                            DesignName = design != null ? design.Name : null
                        }"
                    },

                    Reduce =
                        @"
                        from result in results 
                        group result by result.AccountId into g
                        select new 
                        {
                            AccountId = g.Key,
                            AccountName = g.Where(x=>x.AccountName != null).Select(x=>x.AccountName).FirstOrDefault(),
                            UserName = g.Where(x=>x.UserName != null).Select(x=>x.UserName),
                            DesignName = g.Where(x=>x.DesignName != null).Select(x=>x.DesignName),
                        }",

                    Fields =
                    {
                        {"AccountId" , fieldOptions1 },
                        {"AccountName" , fieldOptions2 },
                        {"DesignName" , fieldOptions2 },
                        {"UserName" , fieldOptions2 }
                    }
                };

                return index;
            }
        }

    }
}

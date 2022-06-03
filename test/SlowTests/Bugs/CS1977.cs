using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class indexes_error_CS1977_Cannot_use_a_lambda_expression_from_reduce : RavenTestBase
    {
        public indexes_error_CS1977_Cannot_use_a_lambda_expression_from_reduce(ITestOutputHelper output) : base(output)
        {
        }

        private class Account
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class User
        {
            public string AccountId { get; set; }
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Design
        {
            public string AccountId { get; set; }
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class ReduceResult
        {
            public string DocumentType { get; set; }
            public string AccountId { get; set; }
            public string AccountName { get; set; }
            public IEnumerable<string> UserName { get; set; }
            public IEnumerable<string> DesignName { get; set; }
        }

        private class ComplexIndex : AbstractMultiMapIndexCreationTask<ReduceResult>
        {
            public ComplexIndex()
            {
                AddMap<Account>(accounts =>
                    from account in accounts
                    select new
                    {
                        DocumentType = "Account",
                        AccountId = account.Id,
                        AccountName = account.Name,
                        DesignName = "",
                        UserName = "",
                    });
                AddMap<Design>(designs =>
                    from design in designs
                    select new
                    {
                        DocumentType = "Design",
                        AccountId = design.AccountId,
                        AccountName = "",
                        DesignName = design.Name,
                        UserName = "",
                    });
                AddMap<User>(users =>
                    from user in users
                    select new
                    {
                        DocumentType = "User",
                        AccountId = user.AccountId,
                        AccountName = "",
                        DesignName = "",
                        UserName = user.Name,
                    });

                Reduce = results =>
                    from result in results
                    group result by result.AccountId into accountGroup
                    from account in accountGroup
                    where account.DocumentType == "Account"
                    select new
                    {
                        DocumentType = "Account",
                        AccountId = account.AccountId,
                        AccountName = account.AccountName,
                        UserName = accountGroup.Where(x => x.DocumentType == "User").SelectMany(x => x.UserName),
                        DesignName = accountGroup.Where(x => x.DocumentType == "Design").SelectMany(x => x.DesignName)
                    };
            }
        }

        private class SelectIndex : AbstractMultiMapIndexCreationTask<ReduceResult>
        {
            public SelectIndex()
            {
                {
                    AddMap<Account>(accounts =>
                        from account in accounts
                        select new
                        {
                            DocumentType = "Account",
                            AccountId = account.Id,
                            AccountName = account.Name,
                            DesignName = "",
                            UserName = "",
                        });
                    AddMap<Design>(designs =>
                        from design in designs
                        select new
                        {
                            DocumentType = "Design",
                            AccountId = design.AccountId,
                            AccountName = "",
                            DesignName = design.Name,
                            UserName = "",
                        });
                    AddMap<User>(users =>
                        from user in users
                        select new
                        {
                            DocumentType = "User",
                            AccountId = user.AccountId,
                            AccountName = "",
                            DesignName = "",
                            UserName = user.Name,
                        });

                    Reduce = results =>
                        from result in results
                        group result by result.AccountId into accountGroup
                        from account in accountGroup
                        where account.DocumentType == "Account"
                        select new
                        {
                            DocumentType = "Account",
                            AccountId = account.AccountId,
                            AccountName = account.AccountName,
                            UserName = accountGroup.Where(x => x.DocumentType == "User").Select(x => x.UserName),
                            DesignName = accountGroup.Where(x => x.DocumentType == "Design").Select(x => x.DesignName)
                        };
                }
            }
        }


        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void can_create_index(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new ComplexIndex().Execute(store);
            }
        }


        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void can_create_index_where_reduce_uses_select(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new SelectIndex().Execute(store);
            }
        }
    }
}

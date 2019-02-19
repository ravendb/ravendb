using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class Accounts : RavenTestBase
    {
        [Fact]
        public void TestLoadAccountByTypeContains()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {

                    var accounts = (from a in session.Query<TestAccount>() where a.Types.Contains(TestAccountTypes.BankAccount) select a).ToList();

                }
            }
        }

        [Fact]
        public void TestLoadAccountByTypeAny()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestAccount(TestAccountTypes.AccountsReceivable, "Test", "Test"));

                    session.SaveChanges();

                    var accounts = (from a in session.Query<TestAccount>() where a.Types.Any(t => t == TestAccountTypes.BankAccount) select a).ToList();

                }
            }
        }

        private enum TestAccountTypes
        {
            Assets = 100000,

            BankAccount = 1000 + Assets,
            AccountsReceivable = 10000 + Assets,
        }


        private class TestAccount
        {
            private TestAccount()
            {
                Types = new List<TestAccountTypes>();
            }

            public TestAccount(TestAccountTypes Type, string Name, string AccountNumber)
                : this()
            {
                this.Name = Name;
                this.AccountNumber = AccountNumber;

                Types.Add(Type);
            }


            public string Name { get; set; }

            public string AccountNumber { get; set; }


            public List<TestAccountTypes> Types { get; set; }
        }
    }
}

using System.Transactions;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class MoreDtcIssues : RavenTest
    {
        public class MyTestClass
        {
            public virtual string Id { get; set; }
            public virtual string SomeText { get; set; }
        }

        [Fact]
        public void CanWriteInTransactionScopeAndReadFromAnotherTransactionScope()
        {
            using (var server = GetNewServer(requestedStorage: "esent"))
            {
                EnsureDtcIsSupported(server);

                using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
                {
                    var testEntity = new MyTestClass() { SomeText = "Foo" };

                    using (var ts = new TransactionScope())
                    {
                        using (var session = store.OpenSession())
                        {
                            session.Store(testEntity);
                            session.SaveChanges();
                        }
                        ts.Complete();
                    }

                    using (var ts = new TransactionScope())
                    {
                        using (var session = store.OpenSession())
                        {
                            session.Advanced.AllowNonAuthoritativeInformation = false;
                            var testEntityRetrieved = session.Load<MyTestClass>(testEntity.Id);
                            Assert.Equal(testEntityRetrieved.SomeText, testEntity.SomeText);
                        }
                    }
                }
            }
        }

        [Fact]
        public void CanWriteInTransactionScopeAndReadOutsideOfTransactionScope()
        {
            using (var server = GetNewServer(requestedStorage: "esent"))
            {
                EnsureDtcIsSupported(server);

                using (var store = new DocumentStore {Url = "http://localhost:8079"}.Initialize())
                {
                    var testEntity = new MyTestClass() {SomeText = "Foo"};

                    using (var ts = new TransactionScope())
                    {
                        using (var session = store.OpenSession())
                        {
                            session.Store(testEntity);
                            session.SaveChanges();
                        }
                        ts.Complete();
                    }

                    using (var session = store.OpenSession())
                    {
                        session.Advanced.AllowNonAuthoritativeInformation = false;
                        var testEntityRetrieved = session.Load<MyTestClass>(testEntity.Id);
                        Assert.Equal(testEntityRetrieved.SomeText, testEntity.SomeText);
                    }
                }
            }
        }
    }
}
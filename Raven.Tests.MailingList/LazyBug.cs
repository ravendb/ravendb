using System;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class LazyBug : RavenTest
    {
        private const string KnownId = "Links/Daniel";


        [Fact]
        public void ShouldLoadLinkAndUserAccountLazily()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var userAccount = new UserAccount
                    {
                        Name = "Daniel"
                    };

                    session.Store(userAccount);
                    session.Store(new ExternalAccount
                    {
                        Id = KnownId,
                        LinkedId = session.Advanced.GetDocumentId(userAccount)
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var lazyExternalAccount =
                        session.Advanced.Lazily
                            .Include<ExternalAccount>(x => x.LinkedId)
                            .Load(KnownId);

                    session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

                    var externalAccount = lazyExternalAccount.Value;

                    Assert.NotNull(externalAccount);
                    Assert.True(session.Advanced.IsLoaded(externalAccount.LinkedId));

                    var account = session.Load<UserAccount>(externalAccount.LinkedId);

                    Assert.IsType<UserAccount>(account);
                }
            }
        }


        public class ExternalAccount
        {
            public string Id { get; set; }
            public string LinkedId { get; set; }
        }

        public class UserAccount
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}

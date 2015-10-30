using System;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

using User = Raven.Tests.Bugs.User;

namespace Raven.SlowTests.Bugs
{
    public class VeryBigResultSetRemote : RavenTest
    {
        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/Esent/MaxVerPages"] = "512";
            configuration.Settings["Raven/Esent/PreferredVerPages"] = "512";
        }

        [Theory]
        [PropertyData("Storages")]
        public void CanGetVeryBigResultSetsEvenThoughItIsBadForYou(string requestedStorage)
        {
            using (var server = GetNewServer(requestedStorage: requestedStorage))
            using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
            {
                store.SetRequestsTimeoutFor(TimeSpan.FromMinutes(3));
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 15000; i++)
                    {
                        session.Store(new User { });
                    }
                    session.SaveChanges();
                }

                server.SystemDatabase.Configuration.MaxPageSize = 20000;

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>()
                        .Customize(x=>x.WaitForNonStaleResults(TimeSpan.FromMinutes(1)))
                        .Take(20000).ToArray();
                    Assert.Equal(15000, users.Length);
                }
            }
        }
    }
}

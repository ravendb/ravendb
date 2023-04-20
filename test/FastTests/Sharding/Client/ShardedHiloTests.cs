using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Identity;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding.Client
{
    public class ShardedHiloTests : RavenTestBase
    {
        public ShardedHiloTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Sharding)]
        public void CanStoreWithoutId()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                string id;
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "Aviv" };
                    session.Store(user);

                    id = user.Id;
                    Assert.NotNull(id);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(id);
                    Assert.Equal("Aviv", loaded.Name);
                }
            }
        }
    }
}

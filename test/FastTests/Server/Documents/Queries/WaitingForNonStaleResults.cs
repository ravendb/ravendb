using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Documents.Queries
{
    [SuppressMessage("ReSharper", "ConsiderUsingConfigureAwait")]
    public class WaitingForNonStaleResults : RavenTestBase
    {
        [Fact]
        public async Task Cutoff_etag_usage()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    var entity = new User();
                    await session.StoreAsync(entity);

                    await session.StoreAsync(new Address());
                    await session.StoreAsync(new Address());

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Name).ToList();

                    Assert.Equal(2, users.Count);
                }
            }
        }

        [Fact]
        public async Task As_of_now_usage()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.StoreAsync(new User());

                    await session.StoreAsync(new Address());
                    await session.StoreAsync(new Address());

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Name).ToList();

                    Assert.Equal(2, users.Count);
                }
            }
        }

        [Fact]
        public void Throws_if_exceeds_timeout()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Store(new Address());
                    session.Store(new Address());

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var sp = Stopwatch.StartNew();
                    Assert.Throws<TimeoutException>(() =>
                        session.Query<Address>()
                        .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMilliseconds(1)))
                        .OrderBy(x => x.City)
                        .ToList()
                    );

                    var timeout = 1000;
                    if (Debugger.IsAttached)
                        timeout *= 25;
                    Assert.True(sp.ElapsedMilliseconds < timeout, sp.Elapsed.ToString());
                }
            }
        }
    }
}

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Documents
{
    public class Load : RavenTestBase
    {
        public Load(ITestOutputHelper output) : base(output)
        {
        }

        private class Foo
        {
            public string Name { get; set; }
        }

        private class Bar
        {
            /*public string FooStringId { get; set; }
            public long FooLongId { get; set; }
            public ulong FooUlongLongId { get; set; }
            public int FooIntId { get; set; }
            public uint FooUIntId { get; set; }
            public short FooShortId { get; set; }
            public ushort FooUShortId { get; set; }
            public byte FooByteId { get; set; }
            */
            public string FooId { get; set; }
            public List<string> FooIDs { get; set; }
            public string Name { get; set; }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void LoadWithIncludes(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                string barId;
                using (var session = store.OpenSession())
                {
                    var foo = new Foo { Name = "Beginning" };
                    session.Store(foo);
                    var fooId = barId = session.Advanced.GetDocumentId(foo);
                    var bar = new Bar
                    {
                        Name = "End",
                        FooId = fooId
                    };
                    session.Store(bar);
                    barId = session.Advanced.GetDocumentId(bar);
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var bar = newSession.Include<Bar>(x=>x.FooId).Load(new[] {barId});
                    Assert.NotNull(bar);
                    Assert.Equal(1, bar.Count);
                    Assert.NotNull(bar[barId]);

                    var numOfRequests = newSession.Advanced.NumberOfRequests;

                    var foo = newSession.Load<Foo>((string)bar[barId].FooId);

                    Assert.NotNull(foo);
                    Assert.Equal("Beginning", foo.Name);
                    Assert.Equal(numOfRequests, newSession.Advanced.NumberOfRequests);
                }

            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void LoadWithIncludesAndMissingDocument(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                string barId;
                using (var session = store.OpenSession())
                {

                    var bar = new Bar
                    {
                        Name = "End",
                        FooId = "somefoo/1"
                    };
                    session.Store(bar);
                    barId = session.Advanced.GetDocumentId(bar);
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var bar = newSession.Include<Bar>(x => x.FooId).Load(new[] { barId });

                    Assert.NotNull(bar);
                    Assert.Equal(1, bar.Count);
                    Assert.NotNull(bar[barId]);

                    var numOfRequests = newSession.Advanced.NumberOfRequests;

                    var foo = newSession.Load<Foo>((string)bar[barId].FooId);

                    Assert.Null(foo);
                    Assert.Equal(numOfRequests, newSession.Advanced.NumberOfRequests);
                }

            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task LoadStartingWith(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var docs = (await session.Advanced.LoadStartingWithAsync<User>("users/")).ToList();
                    Assert.Equal(1, docs.Count);
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        await session.StoreAsync(new User(), "users/");
                    }

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var docs = (await session.Advanced.LoadStartingWithAsync<User>("users/")).ToList();
                    Assert.Equal(6, docs.Count); // fails, returns 1 doc
                }
            }
        }
    }
}

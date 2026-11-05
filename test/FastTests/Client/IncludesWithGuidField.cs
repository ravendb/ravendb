using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client
{
    public class IncludesWithGuidField : RavenTestBase
    {
        public IncludesWithGuidField(ITestOutputHelper output) : base(output)
        {
        }

        private class Profile
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Administrator
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public Guid ProfileId { get; set; }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Can_Include_Guid_Field_Via_Load(Options options)
        {
            using var store = GetDocumentStore(options);

            var profileId = Guid.NewGuid();

            using (var session = store.OpenSession())
            {
                session.Store(new Profile { Id = $"profiles/{profileId}", Name = "Main Profile" });
                session.Store(new Administrator { Name = "John", ProfileId = profileId });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var admins = session.Include<Administrator>(a => a.ProfileId)
                    .Load<Administrator>("administrators/1-A");

                Assert.Equal(1, session.Advanced.NumberOfRequests);

                var profile = session.Load<Profile>($"profiles/{profileId}");

                Assert.Equal(1, session.Advanced.NumberOfRequests);
                Assert.NotNull(profile);
                Assert.Equal("Main Profile", profile.Name);
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Can_Include_Guid_Field_Via_LoadAsync(Options options)
        {
            using var store = GetDocumentStore(options);

            var profileId = Guid.NewGuid();

            using (var session = store.OpenSession())
            {
                session.Store(new Profile { Id = $"profiles/{profileId}", Name = "Main Profile" });
                session.Store(new Administrator { Name = "John", ProfileId = profileId });
                session.SaveChanges();
            }

            using (var session = store.OpenAsyncSession())
            {
                var admins = await session.Include<Administrator>(a => a.ProfileId)
                    .LoadAsync<Administrator>("administrators/1-A");

                Assert.Equal(1, session.Advanced.NumberOfRequests);

                var profile = await session.LoadAsync<Profile>($"profiles/{profileId}");

                Assert.Equal(1, session.Advanced.NumberOfRequests);
                Assert.NotNull(profile);
                Assert.Equal("Main Profile", profile.Name);
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Can_Include_Guid_Field_Via_Query(Options options)
        {
            using var store = GetDocumentStore(options);

            var profileId = Guid.NewGuid();

            using (var session = store.OpenSession())
            {
                session.Store(new Profile { Id = $"profiles/{profileId}", Name = "Main Profile" });
                session.Store(new Administrator { Name = "John", ProfileId = profileId });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var admins = session.Query<Administrator>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Include(a => a.ProfileId)
                    .ToList();

                Assert.Equal(1, session.Advanced.NumberOfRequests);

                var profile = session.Load<Profile>($"profiles/{profileId}");

                Assert.Equal(1, session.Advanced.NumberOfRequests);
                Assert.NotNull(profile);
                Assert.Equal("Main Profile", profile.Name);
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Can_Include_Guid_Field_Via_Lazy_Load(Options options)
        {
            using var store = GetDocumentStore(options);

            var profileId = Guid.NewGuid();

            using (var session = store.OpenSession())
            {
                session.Store(new Profile { Id = $"profiles/{profileId}", Name = "Main Profile" });
                session.Store(new Administrator { Name = "John", ProfileId = profileId });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var lazyAdmin = session.Advanced.Lazily
                    .Include<Administrator>(a => a.ProfileId)
                    .Load<Administrator>("administrators/1-A");

                var admin = lazyAdmin.Value;

                Assert.Equal(1, session.Advanced.NumberOfRequests);

                var profile = session.Load<Profile>($"profiles/{profileId}");

                Assert.Equal(1, session.Advanced.NumberOfRequests);
                Assert.NotNull(profile);
                Assert.Equal("Main Profile", profile.Name);
            }
        }
    }
}

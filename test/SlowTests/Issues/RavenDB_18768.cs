using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18768 : RavenTestBase
{
    public RavenDB_18768(ITestOutputHelper output) : base(output)
    {
    }

    [Theory]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public async Task CanIncludeRevisionsByPathInLoadByIdQuery(Options options)
    {
        const string id = "users/rhino";

        using (var store = GetDocumentStore(options))
        {
            await RevisionsHelper.SetupRevisionsAsync(store);

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Arek", }, id);

                session.SaveChanges();
            }

            string changeVector;
            using (var session = store.OpenSession())
            {
                var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                Assert.Equal(1, metadatas.Count);

                changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query = session.Query<User>().Where(x => x.Id == id)
                    .Include(builder => builder.IncludeRevisions(x => x.FirstRevision));

                query.Customize(c => c.WaitForNonStaleResults()).ToList();

                var revision1 = session.Advanced.Revisions.Get<User>(changeVector);
                Assert.NotNull(revision1);
                Assert.Null(revision1.FirstRevision);

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    [Theory]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public async Task CanIncludeRevisionsBeforeInLoadByIdQuery(Options options)
    {
        const string id = "users/rhino";

        using (var store = GetDocumentStore(options))
        {
            await RevisionsHelper.SetupRevisionsAsync(store);

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Arek", }, id);

                session.SaveChanges();
            }

            string changeVector;
            using (var session = store.OpenSession())
            {
                var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                Assert.Equal(1, metadatas.Count);

                changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
            }

            var beforeDateTime = DateTime.UtcNow;
            using (var session = store.OpenSession())
            {
                var query = session.Query<User>().Where(x => x.Id == id)
                    .Include(builder => builder
                        .IncludeRevisions(beforeDateTime));
                var users = query.Customize(x => x.WaitForNonStaleResults()).ToList();

                var revision = session.Advanced.Revisions.Get<User>(changeVector);

                Assert.NotNull(users);
                Assert.Equal(users.Count, 1);
                Assert.NotNull(revision);
                Assert.NotNull(revision.Name);
                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string ChangeVector { get; set; }
        public string FirstRevision { get; set; }
        public string SecondRevision { get; set; }
        public string ThirdRevision { get; set; }
        public List<string> ChangeVectors { get; set; }
    }
}

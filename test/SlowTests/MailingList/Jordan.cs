using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Jordan : RavenTestBase
    {
        private class DocumentReference
        {
            public string Id { get; set; }
        }

        private class Group
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }


        private class User
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public DocumentReference Group { get; set; }
        }

        private class UserInfo
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public string GroupId { get; set; }
        }
        private class UserIndex : AbstractIndexCreationTask<User, UserIndex.ReduceResult>
        {
            public UserIndex()
            {
                Map = users => from user in users
                               select new
                               {
                                   GroupId = user.Group.Id
                               };

                Store(x => x.GroupId, FieldStorage.Yes);
            }

            public class ReduceResult
            {
                public string GroupId { get; set; }
            }
        }

        [Fact]
        public async Task CanTranslateToRql()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = docStore =>
                {

                    docStore.OnBeforeQuery += (sender, args) => args.QueryCustomization.WaitForNonStaleResults();
                    docStore.Conventions.FindPropertyNameForIndex = (indexedType, indexedName, path, prop) => (path + prop).Replace("[].", "").Replace(".", "");
                }
            }))
            {
                await new UserIndex().ExecuteAsync(store);
                await InsertDocuments(store);
                var results = await RunBuggedQuery(store);
                Assert.Equal(6, results.Count());
                foreach (var item in results)
                {
                    Assert.NotNull(item.GroupId);
                }
            }
        }

        private static async Task<IEnumerable<UserInfo>> RunBuggedQuery(IDocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                var query = session.Query<UserIndex.ReduceResult, UserIndex>()
                    .Where(x => x.GroupId == "groups/1-A")
                    .OfType<User>()
                    .Select(user => new UserInfo
                    {
                        Id = user.Id,
                        FirstName = user.FirstName,
                        LastName = user.LastName,
                        GroupId = user.Group.Id
                    });
                var results = await query.ToListAsync();
                return results;
            }
        }

        private async Task InsertDocuments(IDocumentStore store)
        {
            var radnom = new System.Random(1234);
            using (var session = store.OpenAsyncSession())
            {
                var groupIds = new List<string>();
                for (var i = 0; i < 5; i++)
                {
                    var group = new Group { Name = "Test Group" };
                    await session.StoreAsync(group);
                    groupIds.Add(group.Id);
                }

                for (var i = 0; i < 50; i++)
                {
                    var user = new User
                    {
                        FirstName = "Test",
                        LastName = "User",
                        Group = new DocumentReference
                        {
                            Id = groupIds[radnom.Next(groupIds.Count)]
                        }
                    };
                    await session.StoreAsync(user);
                }

                await session.SaveChangesAsync();
            }
        }


    }
}

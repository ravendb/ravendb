using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Linq
{
    public class CanQueryAndIncludeRevisions : RavenTestBase
    {
        public CanQueryAndIncludeRevisions(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Query_IncludeAllQueryFunctionality()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();

                const string id = "users/rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Omer", },
                        id);

                    session.SaveChanges();
                }

                string changeVector;
                var beforeDateTime = DateTime.UtcNow;
                using (var session = store.OpenSession())
                {
                    var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);

                    session.SaveChanges();

                    cvList.Add(changeVector);

                    metadatas = session.Advanced.Revisions.GetMetadataFor(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);

                    session.SaveChanges();

                    metadatas = session.Advanced.Revisions.GetMetadataFor(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, List<string>>(id, x => x.ChangeVectors, cvList);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Include(builder => builder
                            .IncludeRevisions(x => x.ChangeVectors)
                            .IncludeRevisions(x => x.FirstRevision)
                            .IncludeRevisions(x => x.SecondRevision)).Customize(c => c.WaitForNonStaleResults());


                    var r = query.ToList();

                    var revision1 = session.Advanced.Revisions.Get<User>(cvList[0]);
                    var revision2 = session.Advanced.Revisions.Get<User>(cvList[1]);
                    var revision3 = session.Advanced.Revisions.Get<User>(cvList[2]);

                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_IncludeAllQueryFunctionalityAsync()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();

                const string id = "users/rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Omer", },
                        id);

                    await session.SaveChangesAsync();
                }

                string changeVector;
                var beforeDateTime = DateTime.UtcNow;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);

                    await session.SaveChangesAsync();

                    cvList.Add(changeVector);

                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);

                    await session.SaveChangesAsync();

                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, List<string>>(id, x => x.ChangeVectors, cvList);

                    await session.SaveChangesAsync();
                }

                using (var asyncSession = store.OpenAsyncSession())
                {
                    var query = asyncSession.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Include(builder => builder
                            .IncludeRevisions(x => x.ChangeVectors)
                            .IncludeRevisions(x => x.FirstRevision)
                            .IncludeRevisions(x => x.SecondRevision));


                    var r = await query.ToListAsync();

                    var revision1 = await asyncSession.Advanced.Revisions.GetAsync<User>(cvList[0]);
                    var revision2 = await asyncSession.Advanced.Revisions.GetAsync<User>(cvList[1]);
                    var revision3 = await asyncSession.Advanced.Revisions.GetAsync<User>(cvList[2]);

                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);

                    Assert.Equal(1, asyncSession.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Load_IncludeBuilder_IncludeRevisionByChangeVector()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/rhino";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                string changeVector;
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Omer", },
                        id);
                    session.SaveChanges();

                    var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Load<User>(id, builder => builder.IncludeRevisions(x => x.ChangeVector));
                    var revision = session.Advanced.Revisions.Get<User>(changeVector);

                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Load_IncludeBuilder_IncludeRevisionByChangeVectorAsync()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/rhino";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Omer", },
                        id);
                    await session.SaveChangesAsync();

                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.LoadAsync<User>(id, builder => builder.IncludeRevisions(x => x.ChangeVector));
                    var revision = await session.Advanced.Revisions.GetAsync<User>(changeVector);

                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Load_IncludeBuilder_IncludeRevisionByChangeVectors()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/rhino";
                var cvList = new List<string>();

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Omer", },
                        id);

                    session.SaveChanges();
                }

                string changeVector;
                using (var session = store.OpenSession())
                {
                    var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    session.SaveChanges();
                    cvList.Add(changeVector);

                    metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    cvList.Add(changeVector);

                    session.SaveChanges();
                    metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);
                    session.Advanced.Patch<User, List<string>>(id, x => x.ChangeVectors, cvList);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Load<User>(id, builder => builder.IncludeRevisions(x => x.ChangeVectors));

                    var revision1 = session.Advanced.Revisions.Get<User>(cvList[0]);
                    var revision2 = session.Advanced.Revisions.Get<User>(cvList[1]);
                    var revision3 = session.Advanced.Revisions.Get<User>(cvList[2]);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Load_IncludeBuilder_IncludeRevisionByChangeVectorsAsync()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/rhino";
                var cvList = new List<string>();

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Omer", },
                        id);

                    await session.SaveChangesAsync();
                }

                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);

                    await session.SaveChangesAsync();

                    cvList.Add(changeVector);

                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);

                    await session.SaveChangesAsync();

                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ThirdRevision, changeVector);

                    session.Advanced.Patch<User, List<string>>(id, x => x.ChangeVectors, cvList);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.LoadAsync<User>(id, builder => builder.IncludeRevisions(x => x.ChangeVectors));

                    var revision1 = await session.Advanced.Revisions.GetAsync<User>(cvList[0]);
                    var revision2 = await session.Advanced.Revisions.GetAsync<User>(cvList[1]);
                    var revision3 = await session.Advanced.Revisions.GetAsync<User>(cvList[2]);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Load_IncludeBuilder_IncludeRevisionsByProperty_ChangeVectorAndChangeVectors()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();

                const string id = "users/rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Omer", },
                        id);

                    session.SaveChanges();
                }

                string changeVector;
                var beforeDateTime = DateTime.UtcNow;
                using (var session = store.OpenSession())
                {
                    var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);

                    session.SaveChanges();

                    cvList.Add(changeVector);

                    metadatas = session.Advanced.Revisions.GetMetadataFor(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);

                    session.SaveChanges();

                    metadatas = session.Advanced.Revisions.GetMetadataFor(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, List<string>>(id, x => x.ChangeVectors, cvList);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Load<User>(id, builder => builder
                        .IncludeRevisions(x => x.ChangeVectors)
                        .IncludeRevisions(x => x.FirstRevision)
                        .IncludeRevisions(x => x.SecondRevision));

                    var revision1 = session.Advanced.Revisions.Get<User>(cvList[0]);
                    var revision2 = session.Advanced.Revisions.Get<User>(cvList[1]);
                    var revision3 = session.Advanced.Revisions.Get<User>(cvList[2]);
                    var revisions = session.Advanced.Revisions.Get<User>(cvList);


                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);
                    Assert.NotNull(revisions);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Load_IncludeBuilder_IncludeRevisionsByProperty_ChangeVectorAndChangeVectorsAsync()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();

                const string id = "users/rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Omer", },
                        id);

                    await session.SaveChangesAsync();
                }

                string changeVector;
                var beforeDateTime = DateTime.UtcNow;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);

                    await session.SaveChangesAsync();

                    cvList.Add(changeVector);

                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);

                    await session.SaveChangesAsync();

                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, List<string>>(id, x => x.ChangeVectors, cvList);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var ex = await session.LoadAsync<User>(id, builder => builder
                        .IncludeRevisions(x => x.ChangeVectors)
                        .IncludeRevisions(x => x.FirstRevision)
                        .IncludeRevisions(x => x.SecondRevision));

                    var revision1 = await session.Advanced.Revisions.GetAsync<User>(cvList[0]);
                    var revision2 = await session.Advanced.Revisions.GetAsync<User>(cvList[1]);
                    var revisions = await session.Advanced.Revisions.GetAsync<User>(cvList);

                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revisions);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Load_IncludeBuilder_IncludeRevisionByDateTime_VerifyUtc()
        {
            string changeVector;
            using (var store = GetDocumentStore())
            {
                const string id = "users/rhino";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Omer", },
                        id);
                    session.SaveChanges();

                    var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                    session.Advanced.Patch<User, List<string>>(id, x => x.ChangeVectors, new List<string> { changeVector });
                }

                var dateTime = DateTime.Now.ToLocalTime();
                using (var session = store.OpenSession())
                {
                    var query = session.Load<User>(id, builder => builder
                        .IncludeRevisions(dateTime)
                        .IncludeRevisions(x => x.ChangeVector)
                        .IncludeRevisions(x => x.ChangeVectors));

                    var revision = session.Advanced.Revisions.Get<User>(id, dateTime.ToUniversalTime());
                    var revision2 = session.Advanced.Revisions.Get<User>(changeVector);
                    Assert.NotNull(query);
                    Assert.NotNull(revision);
                    Assert.NotNull(revision2);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Load_IncludeBuilder_IncludeRevisionByDateTime_VerifyUtcAsync()
        {
            string changeVector;
            using (var store = GetDocumentStore())
            {
                const string id = "users/rhino";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Omer", },
                        id);
                    await session.SaveChangesAsync();

                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                }

                var dateTime = DateTime.Now.ToLocalTime();
                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.LoadAsync<User>(id, builder => builder
                        .IncludeRevisions(dateTime)
                        .IncludeRevisions(x => x.ChangeVector));

                    var revision = await session.Advanced.Revisions.GetAsync<User>(id, dateTime.ToUniversalTime());
                    var revision2 = await session.Advanced.Revisions.GetAsync<User>(changeVector);
                    Assert.NotNull(query);
                    Assert.NotNull(revision);
                    Assert.NotNull(revision2);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_IncludeBuilder_IncludeRevisionBefore()
        {
            string changeVector;
            using (var store = GetDocumentStore())
            {
                const string id = "users/rhino";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Omer", },
                        id);
                    session.SaveChanges();

                    var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                }

                var beforeDateTime = DateTime.UtcNow;
                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Include(builder => builder
                            .IncludeRevisions(beforeDateTime));
                    var users = query.ToList();

                    var revision = session.Advanced.Revisions.Get<User>(changeVector);

                    Assert.NotNull(users);
                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_IncludeBuilder_IncludeRevisionBeforeAsync()
        {
            string changeVector;
            using (var store = GetDocumentStore())
            {
                const string id = "users/rhino";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Omer", },
                        id);
                    await session.SaveChangesAsync();

                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                }

                var beforeDateTime = DateTime.UtcNow;
                using (var asyncSession = store.OpenAsyncSession())
                {
                    var query = asyncSession.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Include(builder => builder
                            .IncludeRevisions(beforeDateTime));
                    var users = await query.ToListAsync();

                    var revision = await asyncSession.Advanced.Revisions.GetAsync<User>(changeVector);
                    Assert.NotNull(users);
                    Assert.NotNull(revision);
                    Assert.Equal(1, asyncSession.Advanced.NumberOfRequests);
                }
            }
        }


        [Fact]
        public async Task Query_RawQueryChangeVectorInsidePropertyWithIndex()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Omer", },
                        id);

                    session.SaveChanges();
                }

                string changeVector;
                using (var session = store.OpenSession())
                {
                    var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<User>("from Users where Name = 'Omer' include revisions($p0)")
                        .AddParameter("p0", "ChangeVector")
                        .WaitForNonStaleResults()
                        .ToList();

                    var revision = session.Advanced.Revisions.Get<User>(changeVector);
                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQueryChangeVectorInsidePropertyWithIndexAsync()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Omer", },
                        id);

                    await session.SaveChangesAsync();
                }

                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<User>("from Users where Name = 'Omer' include revisions($p0)")
                        .AddParameter("p0", "ChangeVector")
                        .WaitForNonStaleResults()
                        .ToListAsync();

                    var revision = await session.Advanced.Revisions.GetAsync<User>(changeVector);

                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQueryGetRevisionBeforeDateTime()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                string changeVector;
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Omer", },
                        id);

                    session.SaveChanges();
                    var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                }

                using (var session = store.OpenSession())
                {
                    var getRevisionsBefore = DateTime.UtcNow;
                    var query = session.Advanced
                        .RawQuery<User>("from Users include revisions($p0)")
                        .AddParameter("p0", getRevisionsBefore)
                        .WaitForNonStaleResults()
                        .ToList();

                    var revision = session.Advanced.Revisions.Get<User>(changeVector);

                    Assert.NotNull(query);
                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQueryGetRevisionBeforeDateTimeAsync()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Omer", },
                        id);

                    await session.SaveChangesAsync();
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var getRevisionsBefore = DateTime.UtcNow;
                    var query = await session.Advanced
                        .AsyncRawQuery<User>("from Users include revisions($p0)")
                        .AddParameter("p0", getRevisionsBefore)
                        .WaitForNonStaleResults()
                        .ToListAsync();

                    var revision = await session.Advanced.Revisions.GetAsync<User>(changeVector);

                    Assert.NotNull(query);
                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQuery_IncludeRevisions_Jint_StaticIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                new NameIndex().Execute(store);
                Indexes.WaitForIndexing(store);

                const string id = "users/rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Omer", },
                        id);

                    session.SaveChanges();
                }

                string changeVector;
                using (var session = store.OpenSession())
                {
                    var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<User>(
                            @"
declare function Foo(i) {
    includes.revisions(i.ChangeVector)
    return i;
}
from Users as u
where Name != null 
select Foo(u)"
                        )
                        .WaitForNonStaleResults()
                        .ToList();

                    var revision = session.Advanced.Revisions.Get<User>(changeVector);

                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQuery_IncludeRevisions_Jint_StaticIndexQueryAsync()
        {
            using (var store = GetDocumentStore())
            {
                await new NameIndex().ExecuteAsync(store);
                Indexes.WaitForIndexing(store);
                const string id = "users/rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Omer", },
                        id);

                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);
                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<User>(
                            @"
declare function Foo(i) {
    includes.revisions(i.ChangeVector)
    return i;
}
from Users as u
where Name != null 
select Foo(u)"
                        )
                        .WaitForNonStaleResults()
                        .ToListAsync();

                    var revision = await session.Advanced.Revisions.GetAsync<User>(changeVector);
                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQuery_IncludeRevisions_Jint_IndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Omer", },
                        id);

                    session.SaveChanges();
                }

                string changeVector;
                using (var session = store.OpenSession())
                {
                    var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<User>(
                            @"
declare function Foo(i) {
    includes.revisions(i.ChangeVector)
    return i;
}
from Users as u
where Name != null 
select Foo(u)"
                        )
                        .WaitForNonStaleResults()
                        .ToList();

                    var revision = session.Advanced.Revisions.Get<User>(changeVector);
                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQuery_IncludeRevisions_beforeDateTime_Jint()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                string changeVector;
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Omer", },
                        id);

                    session.SaveChanges();
                    var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                    session.SaveChanges();
                }

                var getRevisionBefore = DateTime.UtcNow;

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<User>(
                            @$"
declare function Foo(i) {{
    includes.revisions('{getRevisionBefore:O}')
    return i;
}}
from Users as u
where ID(u) = 'users/rhino' 
select Foo(u)"
                        )
                        .WaitForNonStaleResults()
                        .ToList();

                    var revision = session.Advanced.Revisions.Get<User>("users/rhino", getRevisionBefore);
                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQuery_IncludeRevisions_beforeDateTime_JintAsync()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/Rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Rhino", },
                        id);

                    await session.SaveChangesAsync();
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                    await session.SaveChangesAsync();
                }

                var getRevisionBefore = DateTime.UtcNow;

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<User>(
                            @$"
declare function Foo(i) {{
    includes.revisions('{getRevisionBefore:O}')
    return i;
}}
from Users as u
where ID(u) = 'users/Rhino' 
select Foo(u)"
                        )
                        .WaitForNonStaleResults()
                        .ToListAsync();

                    var revision = await session.Advanced.Revisions.GetAsync<User>("users/Rhino", getRevisionBefore);
                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQuery_IncludeRevisions_Jint()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/Rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Rhino", },
                        id);

                    session.SaveChanges();
                }

                string changeVector;
                using (var session = store.OpenSession())
                {
                    var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<User>(
                            @"
declare function Foo(i) {
    includes.revisions(i.ChangeVector)
    return i;
}
from Users as u
where ID(u) = 'users/Rhino' 
select Foo(u)"
                        )
                        .WaitForNonStaleResults()
                        .ToList();

                    var revision = session.Advanced.Revisions.Get<User>(changeVector);
                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQuery_IncludeRevisions_JintAsync()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/Rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Rhino", },
                        id);

                    await session.SaveChangesAsync();
                }

                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<User>(
                            @"
declare function Foo(i) {
    includes.revisions(i.ChangeVector)
    return i;
}
from Users as u
where ID(u) = 'users/Rhino' 
select Foo(u)"
                        )
                        .WaitForNonStaleResults()
                        .ToListAsync();

                    var revision = await session.Advanced.Revisions.GetAsync<User>(changeVector);

                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQuery_IncludeRevisionsArray_Jint()
        {
            using (var store = GetDocumentStore())
            {
                new NameIndex().Execute(store);
                Indexes.WaitForIndexing(store);

                const string id = "users/Rhino";
                var cvList = new List<string>();

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Rhino", },
                        id);

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

                    cvList.Add(changeVector);

                    metadatas = session.Advanced.Revisions.GetMetadataFor(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);

                    session.SaveChanges();

                    metadatas = session.Advanced.Revisions.GetMetadataFor(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ThirdRevision, changeVector);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<User>(
                            @"
declare function Foo(i) {
    includes.revisions(i.FirstRevision, i.SecondRevision, i.ThirdRevision)
    return i;
}
from Index 'NameIndex' as u
where u.Name = 'Rhino' 
select Foo(u)"
                        ).WaitForNonStaleResults()
                        .ToList();

                    var revision1 = session.Advanced.Revisions.Get<User>(cvList[0]);
                    if (session.Advanced.NumberOfRequests != 1)
                        WaitForUserToContinueTheTest(store);
                    var revision2 = session.Advanced.Revisions.Get<User>(cvList[1]);
                    if (session.Advanced.NumberOfRequests != 1)
                        WaitForUserToContinueTheTest(store);
                    var revision3 = session.Advanced.Revisions.Get<User>(cvList[2]);
                    if (session.Advanced.NumberOfRequests != 1)
                        WaitForUserToContinueTheTest(store);


                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);

                    if (session.Advanced.NumberOfRequests != 1)
                        WaitForUserToContinueTheTest(store);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQuery_IncludeRevisionsArray_JintAsync()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/Rhino";
                var cvList = new List<string>();

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Rhino", },
                        id);

                    await session.SaveChangesAsync();
                }

                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);

                    await session.SaveChangesAsync();

                    cvList.Add(changeVector);

                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);

                    await session.SaveChangesAsync();

                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ThirdRevision, changeVector);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<User>(
                            @"
declare function Foo(i) {
    includes.revisions(i.FirstRevision, i.SecondRevision, i.ThirdRevision)
    return i;
}
from Users as u
where ID(u) = 'users/Rhino' 
select Foo(u)"
                        )
                        .WaitForNonStaleResults()
                        .ToListAsync();

                    var revision1 = await session.Advanced.Revisions.GetAsync<User>(cvList[0]);
                    var revision2 = await session.Advanced.Revisions.GetAsync<User>(cvList[1]);
                    var revision3 = await session.Advanced.Revisions.GetAsync<User>(cvList[2]);

                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQuery_IncludeRevisionsWithoutAlias()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/Rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Rhino", },
                        id);

                    session.SaveChanges();
                }

                string changeVector;
                using (var session = store.OpenSession())
                {
                    var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<User>("from Users include revisions($p0)")
                        .AddParameter("p0", "ChangeVector")
                        .WaitForNonStaleResults()
                        .ToList();

                    var revision = session.Advanced.Revisions.Get<User>(changeVector);

                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQuery_IncludeRevisionsWithoutAliasAsync()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/Rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Rhino", },
                        id);

                    await session.SaveChangesAsync();
                }

                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<User>("from Users include revisions($p0)")
                        .AddParameter("p0", "ChangeVector")
                        .WaitForNonStaleResults()
                        .ToListAsync();

                    var revision = await session.Advanced.Revisions.GetAsync<User>(changeVector);

                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQueryWithParameters_IncludeRevisions_Array()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();

                const string id = "users/Rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Rhino", },
                        id);

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

                    cvList.Add(changeVector);

                    metadatas = session.Advanced.Revisions.GetMetadataFor(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);

                    session.SaveChanges();

                    metadatas = session.Advanced.Revisions.GetMetadataFor(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ThirdRevision, changeVector);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<User>("from Users include revisions($p0, $p1, $p2)")
                        .AddParameter("p0", "FirstRevision")
                        .AddParameter("p1", "SecondRevision")
                        .AddParameter("p2", "ThirdRevision")
                        .WaitForNonStaleResults()
                        .ToList();

                    var revision1 = session.Advanced.Revisions.Get<User>(cvList[0]);
                    var revision2 = session.Advanced.Revisions.Get<User>(cvList[1]);
                    var revision3 = session.Advanced.Revisions.Get<User>(cvList[2]);

                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQueryWithParameters_IncludeRevisions_Array_SecondOption()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();

                const string id = "users/Rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Rhino", },
                        id);

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

                    cvList.Add(changeVector);

                    metadatas = session.Advanced.Revisions.GetMetadataFor(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);

                    session.SaveChanges();

                    metadatas = session.Advanced.Revisions.GetMetadataFor(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ThirdRevision, changeVector);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<User>("from Users  include revisions(FirstRevision, SecondRevision, ThirdRevision)")
                        .WaitForNonStaleResults()
                        .ToList();

                    var revision1 = session.Advanced.Revisions.Get<User>(cvList[0]);
                    var revision2 = session.Advanced.Revisions.Get<User>(cvList[1]);
                    var revision3 = session.Advanced.Revisions.Get<User>(cvList[2]);

                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQueryWithParameters_IncludeRevisions_Array_SecondOptionAsync()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();

                const string id = "users/Rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Rhino", },
                        id);

                    await session.SaveChangesAsync();
                }

                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);

                    await session.SaveChangesAsync();

                    cvList.Add(changeVector);

                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);

                    await session.SaveChangesAsync();

                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ThirdRevision, changeVector);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<User>("from Users  include revisions(FirstRevision, SecondRevision, ThirdRevision)").WaitForNonStaleResults()
                        .ToListAsync();

                    var revision1 = await session.Advanced.Revisions.GetAsync<User>(cvList[0]);
                    var revision2 = await session.Advanced.Revisions.GetAsync<User>(cvList[1]);
                    var revision3 = await session.Advanced.Revisions.GetAsync<User>(cvList[2]);

                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task Query_RawQueryWithParameters_AliasSyntaxError()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();

                const string id = "users/Rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Rhino", },
                        id);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var error = Assert.Throws<RavenException>(() => session.Advanced
                        .RawQuery<User>("from Users as u include revisions(u.FirstRevision, x.SecondRevision)")
                        .ToList());

                    Assert.Contains("System.InvalidOperationException: Alias is not supported `include revisions(..)`."
                        , error.Message);
                }

                using (var session = store.OpenSession())
                {
                    var error = Assert.Throws<RavenException>(() => session.Advanced
                        .RawQuery<User>("from Users as u include revisions($p0, $p1, $p2)")
                        .AddParameter("p0", "u.FirstRevision")
                        .AddParameter("p1", "u.SecondRevision")
                        .AddParameter("p2", "x.ThirdRevision")
                        .ToList());

                    Assert.Contains("System.InvalidOperationException: Alias is not supported `include revisions(..)`."
                        , error.Message);
                }
            }
        }

        [Fact]
        public async Task Query_RawQueryWithParameters_AliasSyntaxErrorAsync()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();

                const string id = "users/Rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Rhino", },
                        id);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var error = Assert.Throws<RavenException>(() => session.Advanced
                        .RawQuery<User>("from Users as u include revisions(u.FirstRevision, x.SecondRevision)")
                        .ToList());

                    Assert.Contains("System.InvalidOperationException: Alias is not supported `include revisions(..)`."
                        , error.Message);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var error = await Assert.ThrowsAsync<RavenException>(async () => await session.Advanced
                        .AsyncRawQuery<User>("from Users as u include revisions($p0, $p1, $p2)")
                        .AddParameter("p0", "u.FirstRevision")
                        .AddParameter("p1", "u.SecondRevision")
                        .AddParameter("p2", "x.ThirdRevision")
                        .ToListAsync());

                    Assert.Contains("System.InvalidOperationException: Alias is not supported `include revisions(..)`."
                        , error.Message);
                }
            }
        }

        private class User
        {
            public string Name { get; set; }
            public string ChangeVector { get; set; }
            public string FirstRevision { get; set; }
            public string SecondRevision { get; set; }
            public string ThirdRevision { get; set; }
            public List<string> ChangeVectors { get; set; }
        }

        private class NameIndex : AbstractIndexCreationTask<User>
        {
            public NameIndex()
            {
                Map = users => from u in users
                               select new { Name = u.Name, };
            }
        }
    }
}

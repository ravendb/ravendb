using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session.Loaders;
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

        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task Query_IncludeAllQueryFunctionality(bool includeCounters, bool includeTimeSeries)
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

                if (includeCounters)
                {
                    using (var session = store.OpenSession())
                    {
                        var documentCounters = session.CountersFor(id);
                        documentCounters.Increment("Likes", 15);
                        session.SaveChanges();
                    }
                }
                if (includeTimeSeries)
                {
                    using (var session = store.OpenSession())
                    {
                        session.TimeSeriesFor(id, "Hearthrate").Append(DateTime.Now, 15d);
                        session.SaveChanges();
                    }
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Include(builder =>
                        {
                            builder
                                .IncludeRevisions(x => x.ChangeVectors)
                                .IncludeRevisions(x => x.FirstRevision)
                                .IncludeRevisions(x => x.SecondRevision);

                            if (includeCounters)
                                builder.IncludeAllCounters();

                            if (includeTimeSeries)
                                builder.IncludeTimeSeries("Hearthrate");
                        });

                    query.Customize(c => c.WaitForNonStaleResults()).ToList();

                    var revision1 = session.Advanced.Revisions.Get<User>(cvList[0]);
                    var revision2 = session.Advanced.Revisions.Get<User>(cvList[1]);
                    var revision3 = session.Advanced.Revisions.Get<User>(cvList[2]);
                    Assert.NotNull(revision1);
                    Assert.Null(revision1.FirstRevision);
                    Assert.Null(revision1.SecondRevision);
                    Assert.Null(revision1.ChangeVectors);

                    Assert.NotNull(revision2);
                    Assert.NotNull(revision2.FirstRevision);
                    Assert.Null(revision2.SecondRevision);
                    Assert.Null(revision2.ChangeVectors);

                    Assert.NotNull(revision3);
                    Assert.NotNull(revision3.FirstRevision);
                    Assert.NotNull(revision3.SecondRevision);
                    Assert.Null(revision3.ChangeVectors);

                    if (includeCounters)
                    {
                        Assert.Equal(session.CountersFor(id).Get("Likes").Value, 15);
                    }
                    if (includeTimeSeries)
                    {
                        var tf = session.TimeSeriesFor(id, "Hearthrate").Get();
                        Assert.Equal(tf.Length, 1);
                        Assert.Equal(tf[0].Value, 15d);
                    }


                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }


        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task Query_IncludeAllQueryFunctionality_NestedField(bool includeCounters, bool includeTimeSeries)
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();

                const string id = "users/rhino";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenSession())
                {
                    session.Store(new UserNested { Name = "Omer", Revisions = new Revisions()},  id);

                    session.SaveChanges();
                }

                string changeVector;
                var beforeDateTime = DateTime.UtcNow;
                using (var session = store.OpenSession())
                {
                    var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<UserNested, string>(id, x => x.Revisions.FirstRevision, changeVector);

                    session.SaveChanges();

                    cvList.Add(changeVector);

                    metadatas = session.Advanced.Revisions.GetMetadataFor(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);

                    cvList.Add(changeVector);

                    session.Advanced.Patch<UserNested, string>(id, x => x.SecondRevision, changeVector);

                    session.SaveChanges();
                }

                if (includeCounters)
                {
                    using (var session = store.OpenSession())
                    {
                        var documentCounters = session.CountersFor(id);
                        documentCounters.Increment("Likes", 15);
                        session.SaveChanges();
                    }
                }
                if (includeTimeSeries)
                {
                    using (var session = store.OpenSession())
                    {
                        session.TimeSeriesFor(id, "Hearthrate").Append(DateTime.Now, 15d);
                        session.SaveChanges();
                    }
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<UserNested>()
                        .Include(builder =>
                        {
                            builder
                                .IncludeRevisions(x => x.Revisions.FirstRevision)
                                .IncludeRevisions(x => x.SecondRevision);

                            if (includeCounters)
                                builder.IncludeAllCounters();

                            if (includeTimeSeries)
                                builder.IncludeTimeSeries("Hearthrate");
                        });

                    query.Customize(c => c.WaitForNonStaleResults()).ToList();

                    var revision1 = session.Advanced.Revisions.Get<UserNested>(cvList[0]);
                    var revision2 = session.Advanced.Revisions.Get<UserNested>(cvList[1]);
                    Assert.NotNull(revision1);
                    Assert.Null(revision1.Revisions.FirstRevision);

                    Assert.NotNull(revision2);
                    Assert.NotNull(revision2.Revisions.FirstRevision);

                    if (includeCounters)
                    {
                        Assert.Equal(session.CountersFor(id).Get("Likes").Value, 15);
                    }

                    if (includeTimeSeries)
                    {
                        var tf = session.TimeSeriesFor(id, "Hearthrate").Get();
                        Assert.Equal(tf.Length, 1);
                        Assert.Equal(tf[0].Value, 15d);
                    }

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
                    Assert.Null(revision1.FirstRevision);
                    Assert.Null(revision1.SecondRevision);
                    Assert.Null(revision1.ChangeVectors);

                    Assert.NotNull(revision2);
                    Assert.NotNull(revision2.FirstRevision);
                    Assert.Null(revision2.SecondRevision);
                    Assert.Null(revision2.ChangeVectors);

                    Assert.NotNull(revision3);
                    Assert.NotNull(revision3.FirstRevision);
                    Assert.NotNull(revision3.SecondRevision);
                    Assert.Null(revision3.ChangeVectors);

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
                    Assert.Null(revision.FirstRevision);
                    Assert.Null(revision.SecondRevision);
                    Assert.Null(revision.ChangeVectors);

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
                    Assert.Null(revision.FirstRevision);
                    Assert.Null(revision.SecondRevision);
                    Assert.Null(revision.ChangeVectors);
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

                    session.Advanced.Patch<User, List<string>>(id, x => x.ChangeVectors, cvList);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Load<User>(id, builder => builder.IncludeRevisions(x => x.ChangeVectors));

                    var revision1 = session.Advanced.Revisions.Get<User>(cvList[0]);
                    var revision2 = session.Advanced.Revisions.Get<User>(cvList[1]);
                    var revision3 = session.Advanced.Revisions.Get<User>(cvList[2]);

                    Assert.NotNull(revision1);
                    Assert.Null(revision1.FirstRevision);
                    Assert.Null(revision1.SecondRevision);
                    Assert.Null(revision1.ChangeVectors);

                    Assert.NotNull(revision2);
                    Assert.NotNull(revision2.FirstRevision);
                    Assert.Null(revision2.SecondRevision);
                    Assert.Null(revision2.ChangeVectors);

                    Assert.NotNull(revision3);
                    Assert.NotNull(revision3.FirstRevision);
                    Assert.NotNull(revision3.SecondRevision);
                    Assert.Null(revision3.ChangeVectors);

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

                    Assert.NotNull(revision1);
                    Assert.Null(revision1.FirstRevision);
                    Assert.Null(revision1.SecondRevision);
                    Assert.Null(revision1.ChangeVectors);

                    Assert.NotNull(revision2);
                    Assert.NotNull(revision2.FirstRevision);
                    Assert.Null(revision2.SecondRevision);
                    Assert.Null(revision2.ChangeVectors);

                    Assert.NotNull(revision3);
                    Assert.NotNull(revision3.FirstRevision);
                    Assert.NotNull(revision3.SecondRevision);
                    Assert.Null(revision3.ChangeVectors);

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
                    Assert.Null(revision1.FirstRevision);
                    Assert.Null(revision1.SecondRevision);
                    Assert.Null(revision1.ChangeVectors);

                    Assert.NotNull(revision2);
                    Assert.NotNull(revision2.FirstRevision);
                    Assert.Null(revision2.SecondRevision);
                    Assert.Null(revision2.ChangeVectors);

                    Assert.NotNull(revision3);
                    Assert.NotNull(revision3.FirstRevision);
                    Assert.NotNull(revision3.SecondRevision);
                    Assert.Null(revision3.ChangeVectors);

                    Assert.NotNull(revisions);
                    Assert.Equal(revisions.Count, 3);
                    Assert.Contains(cvList[0], revisions.Keys);

                    Assert.NotNull(cvList[0]);
                    Assert.Equal(revisions[cvList[0]].FirstRevision, revision1.FirstRevision);
                    Assert.Equal(revisions[cvList[0]].SecondRevision, revision1.SecondRevision);
                    Assert.Equal(revisions[cvList[0]].ChangeVectors, revision1.ChangeVectors);

                    Assert.Contains(cvList[1], revisions.Keys);
                    Assert.NotNull(cvList[1]);
                    Assert.Equal(revisions[cvList[1]].FirstRevision, revision2.FirstRevision);
                    Assert.Equal(revisions[cvList[1]].SecondRevision, revision2.SecondRevision);
                    Assert.Equal(revisions[cvList[1]].ChangeVectors, revision2.ChangeVectors);

                    Assert.Contains(cvList[2], revisions.Keys);
                    Assert.NotNull(cvList[2]);
                    Assert.Equal(revisions[cvList[2]].FirstRevision, revision3.FirstRevision);
                    Assert.Equal(revisions[cvList[2]].SecondRevision, revision3.SecondRevision);
                    Assert.Equal(revisions[cvList[2]].ChangeVectors, revision3.ChangeVectors);

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
                    Assert.Null(revision1.FirstRevision);
                    Assert.Null(revision1.SecondRevision);
                    Assert.Null(revision1.ChangeVectors);

                    Assert.NotNull(revision2);
                    Assert.NotNull(revision2.FirstRevision);
                    Assert.Null(revision2.SecondRevision);
                    Assert.Null(revision2.ChangeVectors);

                    Assert.NotNull(revisions);
                    Assert.Equal(revisions.Count, 3);
                    Assert.Contains(cvList[0], revisions.Keys);

                    Assert.NotNull(revisions[cvList[0]]);
                    Assert.Equal(revisions[cvList[0]].FirstRevision, revision1.FirstRevision);
                    Assert.Equal(revisions[cvList[0]].SecondRevision, revision1.SecondRevision);
                    Assert.Equal(revisions[cvList[0]].ChangeVectors, revision1.ChangeVectors);

                    Assert.Contains(cvList[1], revisions.Keys);
                    Assert.NotNull(revisions[cvList[1]]);
                    Assert.Equal(revisions[cvList[1]].FirstRevision, revision2.FirstRevision);
                    Assert.Equal(revisions[cvList[1]].SecondRevision, revision2.SecondRevision);
                    Assert.Equal(revisions[cvList[1]].ChangeVectors, revision2.ChangeVectors);

                    Assert.Contains(cvList[2], revisions.Keys);
                    Assert.NotNull(revisions[cvList[2]]);
                    Assert.Equal(revisions[cvList[2]].FirstRevision, ex.FirstRevision);
                    Assert.Equal(revisions[cvList[2]].SecondRevision, ex.SecondRevision);
                    Assert.Null(revisions[cvList[2]].ChangeVectors);

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
                    session.Store(new User { Name = "Omer", }, id);
                    session.SaveChanges();

                    var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                    session.Advanced.Patch<User, List<string>>(id, x => x.ChangeVectors, new List<string> { changeVector });
                    session.SaveChanges();
                }

                var dateTime = DateTime.Now.ToLocalTime();
                using (var session = store.OpenSession())
                {
                    var query = session.Load<User>(id, builder => builder
                        .IncludeRevisions(dateTime)
                        .IncludeRevisions(x => x.ChangeVector)
                        .IncludeRevisions(x => x.ChangeVectors));

                    var revision = session.Advanced.Revisions.Get<User>(changeVector);
                    var revision2 = session.Advanced.Revisions.Get<User>(id, dateTime.ToUniversalTime());
                    Assert.NotNull(query);
                    Assert.Null(query.FirstRevision);
                    Assert.Null(query.SecondRevision);
                    Assert.NotNull(query.ChangeVectors);
                    Assert.NotNull(query.Name);

                    Assert.NotNull(revision);
                    Assert.Null(revision.FirstRevision);
                    Assert.Null(revision.SecondRevision);
                    Assert.Null(revision.ChangeVectors);
                    Assert.NotNull(revision.Name);

                    Assert.NotNull(revision2);
                    Assert.Null(revision2.FirstRevision);
                    Assert.Null(revision2.SecondRevision);
                    Assert.NotNull(revision2.ChangeVectors);
                    Assert.NotNull(revision2.Name);

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
                    var user = new User { Name = "Omer", };
                    await session.StoreAsync(user, id);
                    await session.SaveChangesAsync();

                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    session.Advanced.Patch<User, string>(id, x => x.ChangeVector, changeVector);
                    await session.SaveChangesAsync();
                }

                var dateTime = DateTime.Now.ToLocalTime();
                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.LoadAsync<User>(id, builder => builder
                        .IncludeRevisions(dateTime)
                        .IncludeRevisions(x => x.ChangeVector));

                    var revision2 = await session.Advanced.Revisions.GetAsync<User>(id, dateTime.ToUniversalTime());
                    var revision = await session.Advanced.Revisions.GetAsync<User>(changeVector); //revision before 'ChangeVector' value chaged.
                    Assert.NotNull(query);
                    Assert.Null(query.FirstRevision);
                    Assert.Null(query.SecondRevision);
                    Assert.Null(query.ChangeVectors);
                    Assert.NotNull(query.ChangeVector);
                    Assert.NotNull(query.Name);

                    Assert.NotNull(revision);
                    Assert.Null(revision.FirstRevision);
                    Assert.Null(revision.SecondRevision);
                    Assert.Null(revision.ChangeVectors);
                    Assert.Null(revision.ChangeVector);
                    Assert.NotNull(revision.Name);

                    Assert.NotNull(revision2);
                    Assert.Null(revision2.FirstRevision);
                    Assert.Null(revision2.SecondRevision);
                    Assert.Null(revision2.ChangeVectors);
                    Assert.NotNull(revision2.ChangeVector);
                    Assert.NotNull(revision2.Name);

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
                    Assert.Equal(users.Count, 1);
                    Assert.NotNull(revision);
                    Assert.NotNull(revision.Name);
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
                    Assert.Equal(users.Count, 1);
                    Assert.NotNull(revision);
                    Assert.NotNull(revision.Name);
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
                    Assert.NotNull(revision.Name);
                    Assert.NotNull(query);
                    Assert.Equal(query.Count, 1);
                    Assert.NotNull(query[0].Name);
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
                    Assert.NotNull(revision.Name);
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
                    Assert.Equal(query.Count, 1);
                    Assert.NotNull(query[0].Name);
                    Assert.NotNull(revision);
                    Assert.NotNull(revision.Name);
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
                    Assert.Equal(query.Count, 1);
                    Assert.NotNull(query[0].Name);
                    Assert.NotNull(revision);
                    Assert.NotNull(revision.Name);
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

                    Assert.Equal(query.Count, 1);
                    Assert.NotNull(query[0].Name);
                    Assert.NotNull(revision);
                    Assert.NotNull(revision.Name);
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
                    Assert.Equal(query.Count, 1);
                    Assert.NotNull(query[0].Name);
                    Assert.NotNull(revision);
                    Assert.NotNull(revision.Name);
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
                    Assert.Equal(query.Count, 1);
                    Assert.NotNull(query[0].Name);
                    Assert.NotNull(revision);
                    Assert.NotNull(revision.Name);
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
                    Assert.Equal(query.Count, 1);
                    Assert.NotNull(query[0].Name);
                    Assert.NotNull(revision);
                    Assert.NotNull(revision.Name);
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
                    Assert.Equal(query.Count, 1);
                    Assert.NotNull(query[0].Name);
                    Assert.NotNull(revision);
                    Assert.NotNull(revision.Name);
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
                    Assert.Equal(query.Count, 1);
                    Assert.NotNull(query[0].Name);
                    Assert.NotNull(revision);
                    Assert.NotNull(revision.Name);
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

                    Assert.Equal(query.Count, 1);
                    Assert.NotNull(query[0].Name);
                    Assert.NotNull(revision);
                    Assert.NotNull(revision.Name);
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
                    Assert.Null(revision1.FirstRevision);
                    Assert.Null(revision1.SecondRevision);
                    Assert.Null(revision1.ChangeVectors);

                    Assert.NotNull(revision2);
                    Assert.NotNull(revision2.FirstRevision);
                    Assert.Null(revision2.SecondRevision);
                    Assert.Null(revision2.ChangeVectors);

                    Assert.NotNull(revision3);
                    Assert.NotNull(revision3.FirstRevision);
                    Assert.NotNull(revision3.SecondRevision);
                    Assert.Null(revision3.ChangeVectors);

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
                    Assert.Null(revision1.FirstRevision);
                    Assert.Null(revision1.SecondRevision);
                    Assert.Null(revision1.ChangeVectors);

                    Assert.NotNull(revision2);
                    Assert.NotNull(revision2.FirstRevision);
                    Assert.Null(revision2.SecondRevision);
                    Assert.Null(revision2.ChangeVectors);

                    Assert.NotNull(revision3);
                    Assert.NotNull(revision3.FirstRevision);
                    Assert.NotNull(revision3.SecondRevision);
                    Assert.Null(revision3.ChangeVectors);

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

                    Assert.Equal(query.Count, 1);
                    Assert.NotNull(query[0].Name);
                    Assert.NotNull(revision);
                    Assert.NotNull(revision.Name);
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

                    Assert.Equal(query.Count, 1);
                    Assert.NotNull(query[0].Name);
                    Assert.NotNull(revision);
                    Assert.NotNull(revision.Name);
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
                    Assert.Null(revision1.FirstRevision);
                    Assert.Null(revision1.SecondRevision);
                    Assert.Null(revision1.ChangeVectors);

                    Assert.NotNull(revision2);
                    Assert.NotNull(revision2.FirstRevision);
                    Assert.Null(revision2.SecondRevision);
                    Assert.Null(revision2.ChangeVectors);

                    Assert.NotNull(revision3);
                    Assert.NotNull(revision3.FirstRevision);
                    Assert.NotNull(revision3.SecondRevision);
                    Assert.Null(revision3.ChangeVectors);

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
                    Assert.Null(revision1.FirstRevision);
                    Assert.Null(revision1.SecondRevision);
                    Assert.Null(revision1.ChangeVectors);

                    Assert.NotNull(revision2);
                    Assert.NotNull(revision2.FirstRevision);
                    Assert.Null(revision2.SecondRevision);
                    Assert.Null(revision2.ChangeVectors);

                    Assert.NotNull(revision3);
                    Assert.NotNull(revision3.FirstRevision);
                    Assert.NotNull(revision3.SecondRevision);
                    Assert.Null(revision3.ChangeVectors);

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
                    Assert.Null(revision1.FirstRevision);
                    Assert.Null(revision1.SecondRevision);
                    Assert.Null(revision1.ChangeVectors);

                    Assert.NotNull(revision2);
                    Assert.NotNull(revision2.FirstRevision);
                    Assert.Null(revision2.SecondRevision);
                    Assert.Null(revision2.ChangeVectors);

                    Assert.NotNull(revision3);
                    Assert.NotNull(revision3.FirstRevision);
                    Assert.NotNull(revision3.SecondRevision);
                    Assert.Null(revision3.ChangeVectors);

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

                    Assert.Contains("Field `x.SecondRevision` (which is mentioned inside `include revisions(..)`) is missing in document."
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

                    Assert.Contains("Field `x.ThirdRevision` (which is mentioned inside `include revisions(..)`) is missing in document."
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

                    Assert.Contains("Field `x.SecondRevision` (which is mentioned inside `include revisions(..)`) is missing in document."
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

                    Assert.Contains("Field `x.ThirdRevision` (which is mentioned inside `include revisions(..)`) is missing in document."
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

        private class UserNested
        {
            public string Name { get; set; }
            public string SecondRevision { get; set; }
            public string ThirdRevision { get; set; }

            public Revisions Revisions { get; set; }
        }

        private class Revisions
        {
            public string FirstRevision { get; set; }
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

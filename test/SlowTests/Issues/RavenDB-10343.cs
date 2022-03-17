using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10343 : RavenTestBase
    {
        public RavenDB_10343(ITestOutputHelper output) : base(output)
        {
        }

        private class LatestBuildsIndex : AbstractIndexCreationTask<Build>
        {
            public class Entry
            {
                public string ProductKey { get; set; }
                public string Channel { get; set; }
                public string TeamCityBuildLocalId { get; set; }
                public bool IsPublic { get; set; }
            }

            public LatestBuildsIndex()
            {
                Map = builds => from b in builds
                                select new
                                {
                                    ProductKey = b.ProductKey,
                                    IsPublic = b.IsPublic,
                                    Channel = b.Channel,
                                    TeamCityBuildLocalId = b.TeamCityBuildLocalId
                                };
            }
        }

        private class Build
        {
            public string ProductKey { get; set; }
            public string Channel { get; set; }
            public string TeamCityBuildLocalId { get; set; }
            public bool IsPublic { get; set; }
        }

        private class TeamCityBuild : Build
        {
            public List<string> DownloadsIds { get; set; }
            public DateTime BuildDate { get; set; }
            public NestedObject1 Object1 { get; set; }

        }

        private class BuildDownload : Build
        {
            public int DownloadCount { get; set; }
        }

        private class ProjectionResult
        {
            public TeamCityBuild Build { get; set; }
            public IEnumerable<BuildDownload> Downloads { get; set; }
        }

        private class NestedObject1
        {
            public NestedObject2 Object2 { get; set; }
        }

        private class NestedObject2
        {
            public string Name { get; set; }
        }

        [Fact]
        public void CanQueryWithLoadFromSelectAndProject()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/2-A"
                        }
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/3-A",
                            "buildDownloads/4-A"
                        }
                    }, "teamCityBuilds/2-A");


                    session.Store(new BuildDownload
                    {
                        DownloadCount = 100
                    }, "buildDownloads/1-A");
                    session.Store(new BuildDownload
                    {
                        DownloadCount = 200
                    }, "buildDownloads/2-A");
                    session.Store(new BuildDownload
                    {
                        DownloadCount = 300
                    }, "buildDownloads/3-A");
                    session.Store(new BuildDownload
                    {
                        DownloadCount = 400
                    }, "buildDownloads/4-A");


                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Build>()
                        .Select(entry => RavenQuery.Load<TeamCityBuild>(entry.TeamCityBuildLocalId))
                        .Select(build => new ProjectionResult
                        {
                            Build = build,
                            Downloads = RavenQuery.Load<BuildDownload>(build.DownloadsIds)
                        });

                    Assert.Equal("from 'Builds' as entry " +
                                 "load entry.TeamCityBuildLocalId as build " +
                                 "select { Build : build, Downloads : load(build.DownloadsIds) }"
                        , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);

                    var projectionResult = queryResult[0];
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/2-A" }, projectionResult.Build.DownloadsIds);
                    var downloads = projectionResult.Downloads.ToList();
                    Assert.Equal(2, downloads.Count);
                    Assert.Equal(100, downloads[0].DownloadCount);
                    Assert.Equal(200, downloads[1].DownloadCount);

                    projectionResult = queryResult[1];
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/3-A", "buildDownloads/4-A" }, projectionResult.Build.DownloadsIds);
                    downloads = projectionResult.Downloads.ToList();
                    Assert.Equal(3, downloads.Count);
                    Assert.Equal(100, downloads[0].DownloadCount);
                    Assert.Equal(300, downloads[1].DownloadCount);
                    Assert.Equal(400, downloads[2].DownloadCount);

                }
            }
        }

        [Fact]
        public void CanQueryWithLoadFromSelectAndProjectWhere()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"
                        
                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/3-A",
                        IsPublic = true,
                        Channel = "types/3",
                        TeamCityBuildLocalId = "teamCityBuilds/3-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/2-A"
                        }
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/3-A",
                            "buildDownloads/4-A"                        
                        }
                    }, "teamCityBuilds/2-A");


                    session.Store(new BuildDownload
                    {
                        DownloadCount = 100
                    }, "buildDownloads/1-A");
                    session.Store(new BuildDownload
                    {
                        DownloadCount = 200
                    }, "buildDownloads/2-A");
                    session.Store(new BuildDownload
                    {
                        DownloadCount = 300
                    }, "buildDownloads/3-A");
                    session.Store(new BuildDownload
                    {
                        DownloadCount = 400
                    }, "buildDownloads/4-A");


                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var productBuildKeys = new[] {"products/1-A", "products/2-A"};
                    var buildTypes = new[] { "types/1", "types/2", "types/3" };

                    var query = session.Query<Build>()
                        .Where(entry => entry.ProductKey.In(productBuildKeys) && entry.IsPublic)
                        .Where(entry => entry.Channel.In(buildTypes))
                        .Select(entry => RavenQuery.Load<TeamCityBuild>(entry.TeamCityBuildLocalId))
                        .Select(build => new ProjectionResult
                        {
                            Build = build,
                            Downloads = RavenQuery.Load<BuildDownload>(build.DownloadsIds)
                        });

                    Assert.Equal("from 'Builds' as entry " +
                                 "where (entry.ProductKey in ($p0) and entry.IsPublic = $p1) " +
                                 "and (entry.Channel in ($p2)) " +
                                 "load entry.TeamCityBuildLocalId as build " +
                                 "select { Build : build, Downloads : load(build.DownloadsIds) }"
                        , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);

                    var projectionResult = queryResult[0];
                    Assert.Equal(new [] {"buildDownloads/1-A", "buildDownloads/2-A" }, projectionResult.Build.DownloadsIds);
                    var downloads = projectionResult.Downloads.ToList();
                    Assert.Equal(2, downloads.Count);
                    Assert.Equal(100, downloads[0].DownloadCount);
                    Assert.Equal(200, downloads[1].DownloadCount);

                    projectionResult = queryResult[1];
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/3-A" , "buildDownloads/4-A" }, projectionResult.Build.DownloadsIds);
                    downloads = projectionResult.Downloads.ToList();
                    Assert.Equal(3, downloads.Count);
                    Assert.Equal(100, downloads[0].DownloadCount);
                    Assert.Equal(300, downloads[1].DownloadCount);
                    Assert.Equal(400, downloads[2].DownloadCount);

                }
            }
        }

        [Fact]
        public void CanQueryWithLoadFromSelectAndProjectWhere_UsingSeassionLoad()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/3-A",
                        IsPublic = true,
                        Channel = "types/3",
                        TeamCityBuildLocalId = "teamCityBuilds/3-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/2-A"
                        }
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/3-A",
                            "buildDownloads/4-A"
                        }
                    }, "teamCityBuilds/2-A");


                    session.Store(new BuildDownload
                    {
                        DownloadCount = 100
                    }, "buildDownloads/1-A");
                    session.Store(new BuildDownload
                    {
                        DownloadCount = 200
                    }, "buildDownloads/2-A");
                    session.Store(new BuildDownload
                    {
                        DownloadCount = 300
                    }, "buildDownloads/3-A");
                    session.Store(new BuildDownload
                    {
                        DownloadCount = 400
                    }, "buildDownloads/4-A");


                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var productBuildKeys = new[] { "products/1-A", "products/2-A" };
                    var buildTypes = new[] { "types/1", "types/2", "types/3" };

                    var query = session.Query<Build>()
                        .Where(entry => entry.ProductKey.In(productBuildKeys) && entry.IsPublic)
                        .Where(entry => entry.Channel.In(buildTypes))
                        .Select(entry => session.Load<TeamCityBuild>(entry.TeamCityBuildLocalId))
                        .Select(build => new ProjectionResult
                        {
                            Build = build,
                            Downloads = RavenQuery.Load<BuildDownload>(build.DownloadsIds)
                        });

                    Assert.Equal("from 'Builds' as entry " +
                                 "where (entry.ProductKey in ($p0) and entry.IsPublic = $p1) " +
                                 "and (entry.Channel in ($p2)) " +
                                 "load entry.TeamCityBuildLocalId as build " +
                                 "select { Build : build, Downloads : load(build.DownloadsIds) }"
                        , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);

                    var projectionResult = queryResult[0];
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/2-A" }, projectionResult.Build.DownloadsIds);
                    var downloads = projectionResult.Downloads.ToList();
                    Assert.Equal(2, downloads.Count);
                    Assert.Equal(100, downloads[0].DownloadCount);
                    Assert.Equal(200, downloads[1].DownloadCount);

                    projectionResult = queryResult[1];
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/3-A", "buildDownloads/4-A" }, projectionResult.Build.DownloadsIds);
                    downloads = projectionResult.Downloads.ToList();
                    Assert.Equal(3, downloads.Count);
                    Assert.Equal(100, downloads[0].DownloadCount);
                    Assert.Equal(300, downloads[1].DownloadCount);
                    Assert.Equal(400, downloads[2].DownloadCount);

                }
            }
        }

        [Fact]
        public async Task CanQueryWithLoadFromSelectAndProjectWhereAsync()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/3-A",
                        IsPublic = true,
                        Channel = "types/3",
                        TeamCityBuildLocalId = "teamCityBuilds/3-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/2-A"
                        }
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/3-A",
                            "buildDownloads/4-A"
                        }
                    }, "teamCityBuilds/2-A");


                    session.Store(new BuildDownload
                    {
                        DownloadCount = 100
                    }, "buildDownloads/1-A");
                    session.Store(new BuildDownload
                    {
                        DownloadCount = 200
                    }, "buildDownloads/2-A");
                    session.Store(new BuildDownload
                    {
                        DownloadCount = 300
                    }, "buildDownloads/3-A");
                    session.Store(new BuildDownload
                    {
                        DownloadCount = 400
                    }, "buildDownloads/4-A");


                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var productBuildKeys = new[] { "products/1-A", "products/2-A" };
                    var buildTypes = new[] { "types/1", "types/2", "types/3" };

                    var query = session.Query<Build>()
                        .Where(entry => entry.ProductKey.In(productBuildKeys) && entry.IsPublic)
                        .Where(entry => entry.Channel.In(buildTypes))
                        .Select(entry => RavenQuery.Load<TeamCityBuild>(entry.TeamCityBuildLocalId))
                        .Select(build => new ProjectionResult
                        {
                            Build = build,
                            Downloads = RavenQuery.Load<BuildDownload>(build.DownloadsIds)
                        });

                    Assert.Equal("from 'Builds' as entry " +
                                 "where (entry.ProductKey in ($p0) and entry.IsPublic = $p1) " +
                                 "and (entry.Channel in ($p2)) " +
                                 "load entry.TeamCityBuildLocalId as build " +
                                 "select { Build : build, Downloads : load(build.DownloadsIds) }"
                        , query.ToString());

                    var queryResult = await query.ToListAsync();

                    Assert.Equal(2, queryResult.Count);

                    var projectionResult = queryResult[0];
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/2-A" }, projectionResult.Build.DownloadsIds);
                    var downloads = projectionResult.Downloads.ToList();
                    Assert.Equal(2, downloads.Count);
                    Assert.Equal(100, downloads[0].DownloadCount);
                    Assert.Equal(200, downloads[1].DownloadCount);

                    projectionResult = queryResult[1];
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/3-A", "buildDownloads/4-A" }, projectionResult.Build.DownloadsIds);
                    downloads = projectionResult.Downloads.ToList();
                    Assert.Equal(3, downloads.Count);
                    Assert.Equal(100, downloads[0].DownloadCount);
                    Assert.Equal(300, downloads[1].DownloadCount);
                    Assert.Equal(400, downloads[2].DownloadCount);

                }
            }
        }

        [Fact]
        public void CanQueryFromStaticIndexWithLoadFromSelectAndProject()
        {
            using (var store = GetDocumentStore())
            {
                new LatestBuildsIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/3-A",
                        IsPublic = true,
                        Channel = "types/3",
                        TeamCityBuildLocalId = "teamCityBuilds/3-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/2-A"
                        }
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/3-A",
                            "buildDownloads/4-A"
                        }
                    }, "teamCityBuilds/2-A");


                    session.Store(new BuildDownload
                    {
                        DownloadCount = 100
                    }, "buildDownloads/1-A");
                    session.Store(new BuildDownload
                    {
                        DownloadCount = 200
                    }, "buildDownloads/2-A");
                    session.Store(new BuildDownload
                    {
                        DownloadCount = 300
                    }, "buildDownloads/3-A");
                    session.Store(new BuildDownload
                    {
                        DownloadCount = 400
                    }, "buildDownloads/4-A");


                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var productBuildKeys = new[] { "products/1-A", "products/2-A" };
                    var buildTypes = new[] { "types/1", "types/2", "types/3" };

                    var query = session.Query<LatestBuildsIndex.Entry, LatestBuildsIndex>()
                        .Where(entry => entry.ProductKey.In(productBuildKeys) && entry.IsPublic)
                        .Where(entry => entry.Channel.In(buildTypes))
                        .Select(entry => RavenQuery.Load<TeamCityBuild>(entry.TeamCityBuildLocalId))
                        .Select(build => new ProjectionResult
                        {
                            Build = build,
                            Downloads = RavenQuery.Load<BuildDownload>(build.DownloadsIds)
                        });

                    Assert.Equal("from index 'LatestBuildsIndex' as entry " +
                                 "where (entry.ProductKey in ($p0) and entry.IsPublic = $p1) " +
                                 "and (entry.Channel in ($p2)) " +
                                 "load entry.TeamCityBuildLocalId as build " +
                                 "select { Build : build, Downloads : load(build.DownloadsIds) }"
                        , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);

                    var projectionResult = queryResult[0];
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/2-A" }, projectionResult.Build.DownloadsIds);
                    var downloads = projectionResult.Downloads.ToList();
                    Assert.Equal(2, downloads.Count);
                    Assert.Equal(100, downloads[0].DownloadCount);
                    Assert.Equal(200, downloads[1].DownloadCount);

                    projectionResult = queryResult[1];
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/3-A", "buildDownloads/4-A" }, projectionResult.Build.DownloadsIds);
                    downloads = projectionResult.Downloads.ToList();
                    Assert.Equal(3, downloads.Count);
                    Assert.Equal(100, downloads[0].DownloadCount);
                    Assert.Equal(300, downloads[1].DownloadCount);
                    Assert.Equal(400, downloads[2].DownloadCount);

                }
            }
        }

        [Fact]
        public void CanQueryWithLoadFromSelectWithMemberAccessProjection()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/3-A",
                        IsPublic = true,
                        Channel = "types/3",
                        TeamCityBuildLocalId = "teamCityBuilds/3-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        BuildDate = new DateTime(2016, 6, 6)
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        BuildDate = new DateTime(2018, 1, 1)

                    }, "teamCityBuilds/2-A");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var productBuildKeys = new[] { "products/1-A", "products/2-A" };
                    var buildTypes = new[] { "types/1", "types/2", "types/3" };

                    var query = session.Query<Build>()
                        .Where(entry => entry.ProductKey.In(productBuildKeys) && entry.IsPublic)
                        .Where(entry => entry.Channel.In(buildTypes))
                        .Select(entry => RavenQuery.Load<TeamCityBuild>(entry.TeamCityBuildLocalId))
                        .Select(build => build.BuildDate);

                    Assert.Equal("from 'Builds' as entry " +
                                 "where (entry.ProductKey in ($p0) and entry.IsPublic = $p1) " +
                                 "and (entry.Channel in ($p2)) " +
                                 "load entry.TeamCityBuildLocalId as build " +
                                 "select build.BuildDate"
                        , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);

                    Assert.Equal(new DateTime(2016, 6, 6), queryResult[0]);
                    Assert.Equal(new DateTime(2018, 1, 1), queryResult[1]);

                }
            }
        }

        [Fact]
        public void CanQueryWithLoadFromSelectWithMemberAccessProjection_UsingSessionLoad()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/3-A",
                        IsPublic = true,
                        Channel = "types/3",
                        TeamCityBuildLocalId = "teamCityBuilds/3-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        BuildDate = new DateTime(2016, 6, 6)
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        BuildDate = new DateTime(2018, 1, 1)

                    }, "teamCityBuilds/2-A");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var productBuildKeys = new[] { "products/1-A", "products/2-A" };
                    var buildTypes = new[] { "types/1", "types/2", "types/3" };

                    var query = session.Query<Build>()
                        .Where(entry => entry.ProductKey.In(productBuildKeys) && entry.IsPublic)
                        .Where(entry => entry.Channel.In(buildTypes))
                        .Select(entry => session.Load<TeamCityBuild>(entry.TeamCityBuildLocalId))
                        .Select(build => build.BuildDate);

                    Assert.Equal("from 'Builds' as entry " +
                                 "where (entry.ProductKey in ($p0) and entry.IsPublic = $p1) " +
                                 "and (entry.Channel in ($p2)) " +
                                 "load entry.TeamCityBuildLocalId as build " +
                                 "select build.BuildDate"
                        , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);

                    Assert.Equal(new DateTime(2016, 6, 6), queryResult[0]);
                    Assert.Equal(new DateTime(2018, 1, 1), queryResult[1]);

                }
            }
        }

        [Fact]
        public void CanQueryWithLoadFromSelectWithoutProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/2-A"
                        }
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/3-A",
                            "buildDownloads/4-A"
                        }
                    }, "teamCityBuilds/2-A");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Build>()
                        .Select(entry => RavenQuery.Load<TeamCityBuild>(entry.TeamCityBuildLocalId));

                    Assert.Equal("from 'Builds' as entry " +
                                 "load entry.TeamCityBuildLocalId as __load " +
                                 "select __load"
                        , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/2-A" },
                        queryResult[0].DownloadsIds);
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/3-A", "buildDownloads/4-A" },
                        queryResult[1].DownloadsIds);
                }
            }
        }

        [Fact]
        public void CanQueryWithLoadFromSelectWithoutProjectionWhere()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/2-A"
                        }
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/3-A",
                            "buildDownloads/4-A"
                        }
                    }, "teamCityBuilds/2-A");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var productBuildKeys = new[] { "products/1-A", "products/2-A" };
                    var buildTypes = new[] { "types/1", "types/2", "types/3" };

                    var query = session.Query<Build>()
                        .Where(entry => entry.ProductKey.In(productBuildKeys))
                        .Where(entry => entry.Channel.In(buildTypes))
                        .Select(entry => RavenQuery.Load<TeamCityBuild>(entry.TeamCityBuildLocalId));

                    Assert.Equal("from 'Builds' as entry " +
                                 "where (entry.ProductKey in ($p0)) and (entry.Channel in ($p1)) " +
                                 "load entry.TeamCityBuildLocalId as __load " +
                                 "select __load"
                        , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/2-A" }, 
                        queryResult[0].DownloadsIds);
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/3-A", "buildDownloads/4-A" }, 
                        queryResult[1].DownloadsIds);
                }
            }
        }

        [Fact]
        public void CanQueryWithLoadWithStringParameterFromSelectWithoutProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/2-A"
                        }
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/3-A",
                            "buildDownloads/4-A"
                        }
                    }, "teamCityBuilds/2-A");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var productBuildKeys = new[] { "products/1-A", "products/2-A" };
                    var buildTypes = new[] { "types/1", "types/2", "types/3" };
                    var loadId = "teamCityBuilds/1-A";

                    var query = session.Query<Build>()
                        .Where(entry => entry.ProductKey.In(productBuildKeys))
                        .Where(entry => entry.Channel.In(buildTypes))
                        .Select(entry => RavenQuery.Load<TeamCityBuild>(loadId));

                    Assert.Equal("from 'Builds' as entry " +
                                 "where (entry.ProductKey in ($p1)) and (entry.Channel in ($p2)) " +
                                 "load $p0 as __load " +
                                 "select __load"
                        , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/2-A" },
                        queryResult[0].DownloadsIds);
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/2-A" },
                        queryResult[1].DownloadsIds);
                }
            }
        }

        [Fact]
        public void CanQueryWithLoadWithWrappedConstantParameterFromSelectWithoutProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/2-A"
                        }
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/3-A",
                            "buildDownloads/4-A"
                        }
                    }, "teamCityBuilds/2-A");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var productBuildKeys = new[] { "products/1-A", "products/2-A" };
                    var buildTypes = new[] { "types/1", "types/2", "types/3" };
                    var obj = new
                    {
                        Foo = "Bar",
                        Id = "teamCityBuilds/1-A"
                    };

                    var query = session.Query<Build>()
                        .Where(entry => entry.ProductKey.In(productBuildKeys))
                        .Where(entry => entry.Channel.In(buildTypes))
                        .Select(entry => RavenQuery.Load<TeamCityBuild>(obj.Id));

                    Assert.Equal("from 'Builds' as entry " +
                                 "where (entry.ProductKey in ($p1)) and (entry.Channel in ($p2)) " +
                                 "load $p0 as __load " +
                                 "select __load"
                        , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/2-A" },
                        queryResult[0].DownloadsIds);
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/2-A" },
                        queryResult[1].DownloadsIds);
                }
            }
        }

        [Fact]
        public void CanQueryFromStaticIndexWithLoadFromSelectWithoutProjection()
        {
            using (var store = GetDocumentStore())
            {
                new LatestBuildsIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/3-A",
                        IsPublic = true,
                        Channel = "types/3",
                        TeamCityBuildLocalId = "teamCityBuilds/3-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/2-A"
                        }
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/3-A",
                            "buildDownloads/4-A"
                        }
                    }, "teamCityBuilds/2-A");


                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var productBuildKeys = new[] { "products/1-A", "products/2-A" };
                    var buildTypes = new[] { "types/1", "types/2", "types/3" };

                    var query = session.Query<LatestBuildsIndex.Entry, LatestBuildsIndex>()
                        .Where(entry => entry.ProductKey.In(productBuildKeys))
                        .Where(entry => entry.Channel.In(buildTypes))
                        .Select(entry => RavenQuery.Load<TeamCityBuild>(entry.TeamCityBuildLocalId));

                    Assert.Equal("from index 'LatestBuildsIndex' as entry " +
                                 "where (entry.ProductKey in ($p0)) and (entry.Channel in ($p1)) " +
                                 "load entry.TeamCityBuildLocalId as __load " +
                                 "select __load"
                        , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/2-A" },
                        queryResult[0].DownloadsIds);
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/3-A", "buildDownloads/4-A" },
                        queryResult[1].DownloadsIds);
                }
            }
        }

        [Fact]
        public void CanQueryFromStaticIndexWithLoadFromSelectWithoutProjection_UsingSessionLoad()
        {
            using (var store = GetDocumentStore())
            {
                new LatestBuildsIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/3-A",
                        IsPublic = true,
                        Channel = "types/3",
                        TeamCityBuildLocalId = "teamCityBuilds/3-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/2-A"
                        }
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/3-A",
                            "buildDownloads/4-A"
                        }
                    }, "teamCityBuilds/2-A");


                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var productBuildKeys = new[] { "products/1-A", "products/2-A" };
                    var buildTypes = new[] { "types/1", "types/2", "types/3" };

                    var query = session.Query<LatestBuildsIndex.Entry, LatestBuildsIndex>()
                        .Where(entry => entry.ProductKey.In(productBuildKeys))
                        .Where(entry => entry.Channel.In(buildTypes))
                        .Select(entry => session.Load<TeamCityBuild>(entry.TeamCityBuildLocalId));

                    Assert.Equal("from index 'LatestBuildsIndex' as entry " +
                                 "where (entry.ProductKey in ($p0)) and (entry.Channel in ($p1)) " +
                                 "load entry.TeamCityBuildLocalId as __load " +
                                 "select __load"
                        , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/2-A" },
                        queryResult[0].DownloadsIds);
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/3-A", "buildDownloads/4-A" },
                        queryResult[1].DownloadsIds);
                }
            }
        }

        [Fact]
        public async Task CanQueryFromStaticIndexWithLoadFromSelectWithoutProjectionAsync()
        {
            using (var store = GetDocumentStore())
            {
                new LatestBuildsIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/3-A",
                        IsPublic = true,
                        Channel = "types/3",
                        TeamCityBuildLocalId = "teamCityBuilds/3-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/2-A"
                        }
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        DownloadsIds = new List<string>
                        {
                            "buildDownloads/1-A",
                            "buildDownloads/3-A",
                            "buildDownloads/4-A"
                        }
                    }, "teamCityBuilds/2-A");


                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var productBuildKeys = new[] { "products/1-A", "products/2-A" };
                    var buildTypes = new[] { "types/1", "types/2", "types/3" };

                    var query = session.Query<LatestBuildsIndex.Entry, LatestBuildsIndex>()
                        .Where(entry => entry.ProductKey.In(productBuildKeys))
                        .Where(entry => entry.Channel.In(buildTypes))
                        .Select(entry => RavenQuery.Load<TeamCityBuild>(entry.TeamCityBuildLocalId));

                    Assert.Equal("from index 'LatestBuildsIndex' as entry " +
                                 "where (entry.ProductKey in ($p0)) and (entry.Channel in ($p1)) " +
                                 "load entry.TeamCityBuildLocalId as __load " +
                                 "select __load"
                        , query.ToString());

                    var queryResult = await query.ToListAsync();

                    Assert.Equal(2, queryResult.Count);
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/2-A" },
                        queryResult[0].DownloadsIds);
                    Assert.Equal(new[] { "buildDownloads/1-A", "buildDownloads/3-A", "buildDownloads/4-A" },
                        queryResult[1].DownloadsIds);
                }
            }
        }

        [Fact]
        public void CanSelectMemberAccessOfLoadedDocument()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        BuildDate = new DateTime(2016, 6, 6)
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        BuildDate = new DateTime(2018, 1, 1)
                    }, "teamCityBuilds/2-A");


                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Build>()
                        .Select(entry => 
                            RavenQuery.Load<TeamCityBuild>(entry.TeamCityBuildLocalId)
                            .BuildDate);


                    Assert.Equal("from 'Builds' as entry " +
                                 "load entry.TeamCityBuildLocalId as __load " +
                                 "select __load.BuildDate"
                        , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);

                    Assert.Equal(new DateTime(2016, 6, 6), queryResult[0]);

                    Assert.Equal(new DateTime(2018, 1, 1), queryResult[1]);

                }
            }
        }

        [Fact]
        public void CanSelectNestedMemberOfLoadedDocument()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new TeamCityBuild
                    {
                        Object1 = new NestedObject1
                        {
                            Object2 = new NestedObject2
                            {
                                Name = "Aviv"
                            }
                        }
                        
                    }, "teamCityBuilds/1-A");


                    session.Store(new TeamCityBuild
                    {
                        Object1 = new NestedObject1
                        {
                            Object2 = new NestedObject2
                            {
                                Name = "Arek"
                            }
                        }

                    }, "teamCityBuilds/2-A");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Build>()
                        .Select(entry =>
                            RavenQuery.Load<TeamCityBuild>(entry.TeamCityBuildLocalId)
                            .Object1.Object2.Name);


                    Assert.Equal("from 'Builds' as entry " +
                                 "load entry.TeamCityBuildLocalId as __load " +
                                 "select __load.Object1.Object2.Name"
                        , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);

                    Assert.Equal("Aviv", queryResult[0]);

                    Assert.Equal("Arek", queryResult[1]);

                }
            }
        }

        [Fact]
        public void CanSelectMemberAccessOfLoadedDocumentWhere()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/3-A",
                        IsPublic = false,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"
                    });

                    session.Store(new TeamCityBuild
                    {
                        BuildDate = new DateTime(2016, 6, 6)
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        BuildDate = new DateTime(2018, 1, 1)
                    }, "teamCityBuilds/2-A");


                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Build>()
                        .Where(e => e.IsPublic)
                        .Select(entry =>
                            RavenQuery.Load<TeamCityBuild>(entry.TeamCityBuildLocalId)
                            .BuildDate);


                    Assert.Equal("from 'Builds' as entry " +
                                 "where entry.IsPublic = $p0 " +
                                 "load entry.TeamCityBuildLocalId as __load " +
                                 "select __load.BuildDate"
                        , query.ToString());

                    var queryResult = query.ToList();

                    Assert.Equal(2, queryResult.Count);

                    Assert.Equal(new DateTime(2016, 6, 6), queryResult[0]);

                    Assert.Equal(new DateTime(2018, 1, 1), queryResult[1]);

                }
            }
        }

        [Fact]
        public async Task CanSelectMemberAccessOfLoadedDocumentWhereAsync()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Build
                    {
                        ProductKey = "products/1-A",
                        IsPublic = true,
                        Channel = "types/1",
                        TeamCityBuildLocalId = "teamCityBuilds/1-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/2-A",
                        IsPublic = true,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"

                    });

                    session.Store(new Build
                    {
                        ProductKey = "products/3-A",
                        IsPublic = false,
                        Channel = "types/2",
                        TeamCityBuildLocalId = "teamCityBuilds/2-A"
                    });

                    session.Store(new TeamCityBuild
                    {
                        BuildDate = new DateTime(2016, 6, 6)
                    }, "teamCityBuilds/1-A");

                    session.Store(new TeamCityBuild
                    {
                        BuildDate = new DateTime(2018, 1, 1)
                    }, "teamCityBuilds/2-A");


                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<Build>()
                        .Where(e => e.IsPublic)
                        .Select(entry =>
                            RavenQuery.Load<TeamCityBuild>(entry.TeamCityBuildLocalId)
                            .BuildDate);


                    Assert.Equal("from 'Builds' as entry " +
                                 "where entry.IsPublic = $p0 " +
                                 "load entry.TeamCityBuildLocalId as __load " +
                                 "select __load.BuildDate"
                        , query.ToString());

                    var queryResult = await query.ToListAsync();

                    Assert.Equal(2, queryResult.Count);

                    Assert.Equal(new DateTime(2016, 6, 6), queryResult[0]);

                    Assert.Equal(new DateTime(2018, 1, 1), queryResult[1]);

                }
            }
        }

    }
}


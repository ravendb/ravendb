using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents
{
    public class Collections : RavenTestBase
    {
        public Collections(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.None)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanSurviveRestart(Options options)
        {
            var path = NewDataPath();
            options.Path = path;
            var name = "CanSurviveRestart_" + Guid.NewGuid();
            options.ModifyDatabaseName = _ => name;

            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    commands.Put("orders/1", null, new { }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "Orders"}
                    });

                    commands.Put("orders/2", null, new { }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "orders"}
                    });

                    commands.Put("people/1", null, new { }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "People"}
                    });

                    var collectionStats = store.Maintenance.Send(new GetCollectionStatisticsOperation());

                    Assert.Equal(2, collectionStats.Collections.Count);

                    var orders = collectionStats.Collections.First(x => x.Key == "Orders");
                    Assert.Equal(2, orders.Value);

                    var people = collectionStats.Collections.First(x => x.Key == "People");
                    Assert.Equal(1, people.Value);
                }
            }

            switch (options.DatabaseMode)
            {
                case RavenDatabaseMode.Single:
                    AssertForDbLockFile(path);
                    break;
                case RavenDatabaseMode.Sharded:
                    var folders = Directory.GetDirectories(path);
                    foreach (string folder in folders)
                    {
                        if (Path.GetFileName(folder).StartsWith(name) == false)
                            continue;

                        AssertForDbLockFile(folder);
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            using (var store = GetDocumentStore(options))
            {
                var collectionStats = store.Maintenance.Send(new GetCollectionStatisticsOperation());

                Assert.Equal(2, collectionStats.Collections.Count);

                var orders = collectionStats.Collections.First(x => x.Key == "Orders");
                Assert.Equal(2, orders.Value);

                var people = collectionStats.Collections.First(x => x.Key == "People");
                Assert.Equal(1, people.Value);
            }
        }

        private static void AssertForDbLockFile(string path)
        {
            for (int i = 0; i < 15; i++)
            {
                if (File.Exists(Path.Combine(path, "db.lock")) == false)
                    break;
                Thread.Sleep(50);
            }

            Assert.False(File.Exists(Path.Combine(path, "db.lock")), "The database lock file was still there?");
        }
    }
}

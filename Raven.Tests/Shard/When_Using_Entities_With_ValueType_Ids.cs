using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Client.Shard.ShardStrategy;
using Raven.Client.Shard.ShardStrategy.ShardAccess;
using Raven.Client.Shard.ShardStrategy.ShardResolution;
using Raven.Client.Shard.ShardStrategy.ShardSelection;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Shard
{
    public class When_Using_Entities_With_ValueType_Ids : RemoteClientTest, IDisposable
    {
        private readonly string path1;
        private readonly string path2;
        private readonly int port1;
        private readonly int port2;
        private readonly string server;
        private readonly RavenDbServer server1;
        private readonly RavenDbServer server2;
        private readonly ShardStrategy shardStrategy;
        private Shards shards;

        public When_Using_Entities_With_ValueType_Ids()
        {
            server = "localhost";

            port1 = 8080;
            port2 = 8081;

            path1 = GetPath("TestShardedDb1");
            path2 = GetPath("TestShardedDb2");

            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port1);
            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port2);

            server1 = GetNewServer(port1, path1);
            server2 = GetNewServer(port2, path2);

            RefreshShards();

            shardStrategy = new ShardStrategy
                                {
                                    ShardAccessStrategy = new ParallelShardAccessStrategy(),
                                    ShardResolutionStrategy = new ByModulusResolutionStrategy("Shard", serverCount: 2),
                                    ShardSelectionStrategy = new ByModulusSelectionStrategy("Shard", serverCount: 2)
                                };

            using (IDocumentStore store = new ShardedDocumentStore(shardStrategy, shards).Initialize())
            {
                var car1 = new Car {Id = 1, Name = "Car 1"};
                var car2 = new Car {Id = 2, Name = "Car 2"};
                var car3 = new Car {Id = 3, Name = "Car 3"};

                using (IDocumentSession session = store.OpenSession())
                {
                    session.Store(car1);
                    session.Store(car2);
                    session.Store(car3);
                    session.SaveChanges();
                }
            }
        }

        private void RefreshShards()
        {
            shards = new Shards
                         {
                             new DocumentStore {Identifier = "Shard1", Url = "http://" + server + ":" + port1},
                             new DocumentStore {Identifier = "Shard2", Url = "http://" + server + ":" + port2}
                         };
        }

        #region IDisposable Members

        public void Dispose()
        {
            server1.Dispose();
            server2.Dispose();

            Thread.Sleep(100);

            foreach (string path in new[] {path1, path2})
            {
                try
                {
                    IOExtensions.DeleteDirectory(path);
                }
                catch (Exception)
                {
                }
            }
        }

        #endregion

        [Fact]
        public void Can_load_an_entity_by_id_instead_of_database_string_key()
        {
            RefreshShards();

            using (IDocumentStore store = new ShardedDocumentStore(shardStrategy, shards).Initialize())
            using (IDocumentSession session = store.OpenSession())
            {
                var car = session.Load<Car>(3);
                Assert.NotNull(car);
                Assert.Equal("Car 3", car.Name);
            }
        }
    }

    public class ByModulusSelectionStrategy : IShardSelectionStrategy
    {
        private readonly uint serverCount;
        private readonly string shardPrefix;

        public ByModulusSelectionStrategy(string shardPrefix, uint serverCount)
        {
            this.serverCount = serverCount;
            this.shardPrefix = shardPrefix;
        }

        #region IShardSelectionStrategy Members

        string IShardSelectionStrategy.ShardIdForNewObject(object obj)
        {
            var car = obj as Car;

            if (car != null)
                return GetShardId(car);

            throw new NotImplementedException();
        }

        string IShardSelectionStrategy.ShardIdForExistingObject(object obj)
        {
            var car = obj as Car;

            if (car != null)
                return GetShardId(car);

            throw new NotImplementedException();
        }

        #endregion

        private string GetShardId(Car car)
        {
            return shardPrefix + ModulusHelper.GetPositiveGroupNumber(car.Id, serverCount);
        }
    }

    public class ByModulusResolutionStrategy : IShardResolutionStrategy
    {
        private readonly uint serverCount;
        private readonly string shardPrefix;

        public ByModulusResolutionStrategy(string shardPrefix, uint serverCount)
        {
            this.shardPrefix = shardPrefix;
            this.serverCount = serverCount;
        }

        #region IShardResolutionStrategy Members

        IList<string> IShardResolutionStrategy.SelectShardIds(ShardResolutionStrategyData srsd)
        {
            if (srsd.HasValueTypeKey)
            {
                var key = (int) srsd.ValueTypeKey;
                return new[] {shardPrefix + ModulusHelper.GetPositiveGroupNumber(key, serverCount)};
            }

            throw new NotImplementedException();
        }

        #endregion
    }

    internal class ModulusHelper
    {
        public static int GetPositiveGroupNumber(int x, uint numberOfGroups)
        {
            long remainder = x%numberOfGroups;
            remainder = remainder < 0 ? remainder + numberOfGroups : remainder;
            return (int) (remainder + 1);
        }
    }

    internal class Car
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }
}
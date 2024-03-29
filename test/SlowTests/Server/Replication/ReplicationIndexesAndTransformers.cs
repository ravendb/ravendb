﻿using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class ReplicationIndexesAndTransformers : ReplicationTestBase
    {
        public ReplicationIndexesAndTransformers(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name { get; set; }
            public int Age { get; set; }
            public DateTime Birthday { get; set; }
        }

        private class UserByNameAndBirthday : AbstractIndexCreationTask<User>
        {
            public UserByNameAndBirthday()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name,
                                   user.Birthday
                               };
            }
        }

        private class UserByNameIndex : AbstractIndexCreationTask<User>
        {
            private readonly string _indexName;

            public override string IndexName =>
                string.IsNullOrEmpty(_indexName) ? base.IndexName : _indexName;

            public UserByNameIndex(string name = null)
            {
                _indexName = name;
                Map = users => from user in users
                               select new
                               {
                                   user.Name
                               };
            }
        }

        private class UserByAgeIndex : AbstractIndexCreationTask<User>
        {
            private readonly string _indexName;

            public override string IndexName =>
                string.IsNullOrEmpty(_indexName) ? base.IndexName : _indexName;

            public UserByAgeIndex(string name = null)
            {
                _indexName = name;
                Map = users => from user in users
                               select new
                               {
                                   user.Age
                               };
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Indexes)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Can_replicate_index(Options options)
        {
            var (source, destination) = await CreateDuoCluster(options);

            using (source)
            using (destination)
            {
                var userByAge = new UserByAgeIndex();
                userByAge.Execute(source);

                var sw = Stopwatch.StartNew();
                var destIndexNames = new string[0];
                var timeout = Debugger.IsAttached ? 60 * 1000000 : 3000;
                while (sw.ElapsedMilliseconds < timeout && destIndexNames.Length != 1)
                    destIndexNames = destination.Maintenance.Send(new GetIndexNamesOperation(0, 1024));

                Assert.NotNull(destIndexNames); //precaution
                Assert.Equal(1, destIndexNames.Length);
                Assert.Equal(userByAge.IndexName, destIndexNames.First());
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Indexes)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Can_replicate_multiple_indexes(Options options)
        {
            var (source, destination) = await CreateDuoCluster(options);

            using (source)
            using (destination)
            {
                var userByAge = new UserByAgeIndex();
                userByAge.Execute(source);

                var userByName = new UserByNameIndex();
                userByName.Execute(source);

                var sw = Stopwatch.StartNew();
                var destIndexNames = new string[0];
                var timeout = Debugger.IsAttached ? 60 * 1000000 : 3000;
                while (sw.ElapsedMilliseconds < timeout && destIndexNames.Length != 2)
                    destIndexNames = destination.Maintenance.Send(new GetIndexNamesOperation(0, 1024));

                Assert.NotNull(destIndexNames); //precaution
                Assert.Equal(2, destIndexNames.Length);
                Assert.True(destIndexNames.Contains(userByAge.IndexName));
                Assert.True(destIndexNames.Contains(userByName.IndexName));
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Indexes)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Can_replicate_multiple_indexes_and_multiple_transformers(Options options)
        {
            var (source, destination) = await CreateDuoCluster(options);

            using (source)
            using (destination)
            {
                var userByAge = new UserByAgeIndex();
                userByAge.Execute(source);

                var userByName = new UserByNameIndex();
                userByName.Execute(source);

                var sw = Stopwatch.StartNew();
                var destIndexNames = new string[0];
                var timeout = Debugger.IsAttached ? 60 * 1000000 : 3000;
                while (sw.ElapsedMilliseconds < timeout && destIndexNames.Length != 2)
                    destIndexNames = destination.Maintenance.Send(new GetIndexNamesOperation(0, 1024));

                sw.Restart();

                Assert.NotNull(destIndexNames); //precaution
                Assert.Equal(2, destIndexNames.Length);
                Assert.True(destIndexNames.Contains(userByAge.IndexName));
                Assert.True(destIndexNames.Contains(userByName.IndexName));
            }
        }
    }
}




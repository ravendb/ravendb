using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Transformers;
using Raven.Client.Documents.Transformers;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationIndexesAndTransformers : ReplicationTestsBase
    {
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

        private class UsernameToUpperTransformer : AbstractTransformerCreationTask<User>
        {
            private readonly string _transformerName;

            public override string TransformerName =>
                string.IsNullOrEmpty(_transformerName) ? base.TransformerName : _transformerName;

            public UsernameToUpperTransformer(string transformerName = null)
            {
                _transformerName = transformerName;
                TransformResults = users => from user in users
                                            select new
                                            {
                                                Name = user.Name.ToUpper(),
                                                user.Age,
                                                user.Birthday
                                            };
            }
        }

        private class UsernameToLowerTransformer : AbstractTransformerCreationTask<User>
        {
            private readonly string _transformerName;

            public override string TransformerName =>
                string.IsNullOrEmpty(_transformerName) ? base.TransformerName : _transformerName;

            public UsernameToLowerTransformer(string transformerName = null)
            {
                _transformerName = transformerName;
                TransformResults = users => from user in users
                                            select new
                                            {
                                                Name = user.Name.ToLower(),
                                                user.Age,
                                                user.Birthday
                                            };
            }
        }

        public async Task<(DocumentStore source, DocumentStore destination)> CreateDuoCluster([CallerMemberName] string caller = null)
        {
            var leader = await CreateRaftClusterAndGetLeader(2);
            var follower = Servers.First(srv => ReferenceEquals(srv, leader) == false);
            var source = new DocumentStore
            {
                Urls = leader.WebUrls,
                Database = caller
            };
            var destination = new DocumentStore
            {
                Urls = follower.WebUrls,
                Database = caller
            };

            var res  = CreateClusterDatabase(caller, source, 2);
            //var doc = MultiDatabase.CreateDatabaseDocument(dbName);
            //var databaseResult = source.Admin.Server.Send(new CreateDatabaseOperation(doc, 2));
            await WaitForRaftIndexToBeAppliedInCluster(res.ETag ?? 0, TimeSpan.FromSeconds(5));
            return (source, destination);
        }
        
        [Fact]
        public async Task Can_replicate_index()
        {
            var (source, destination) = await CreateDuoCluster();
            
            using (source)
            using (destination)
            {

                var userByAge = new UserByAgeIndex();
                userByAge.Execute(source);

                var sw = Stopwatch.StartNew();
                var destIndexNames = new string[0];
                var timeout = Debugger.IsAttached ? 60 * 1000000 : 3000;
                while (sw.ElapsedMilliseconds < timeout && destIndexNames.Length != 1)
                    destIndexNames = destination.Admin.Send(new GetIndexNamesOperation(0, 1024));

                Assert.NotNull(destIndexNames); //precaution
                Assert.Equal(1, destIndexNames.Length);
                Assert.Equal(userByAge.IndexName, destIndexNames.First());
            }
        }

        [Fact]
        public async Task Can_replicate_multiple_indexes()
        {
            var (source, destination) = await CreateDuoCluster();

            using (source)
            using (destination)
            {
                var userByAge = new UserByAgeIndex();
                userByAge.Execute(source);

                var userByName = new UserByNameIndex();
                userByName.Execute(source);

                await SetupReplicationAsync(source, destination);

                var sw = Stopwatch.StartNew();
                var destIndexNames = new string[0];
                var timeout = Debugger.IsAttached ? 60 * 1000000 : 3000;
                while (sw.ElapsedMilliseconds < timeout && destIndexNames.Length != 2)
                    destIndexNames = destination.Admin.Send(new GetIndexNamesOperation(0, 1024));

                Assert.NotNull(destIndexNames); //precaution
                Assert.Equal(2, destIndexNames.Length);
                Assert.True(destIndexNames.Contains(userByAge.IndexName));
                Assert.True(destIndexNames.Contains(userByName.IndexName));
            }
        }

        [Fact]
        public async Task Can_replicate_multiple_indexes_and_multiple_transformers()
        {
            var (source, destination) = await CreateDuoCluster();

            using (source)
            using (destination)
            {
                var userByAge = new UserByAgeIndex();
                userByAge.Execute(source);

                var usernameToUpperTransformer = new UsernameToUpperTransformer();
                usernameToUpperTransformer.Execute(source);

                var userByName = new UserByNameIndex();
                userByName.Execute(source);

                var usernameToLowerTransformer = new UsernameToLowerTransformer();
                usernameToLowerTransformer.Execute(source);

                await SetupReplicationAsync(source, destination);

                var sw = Stopwatch.StartNew();
                var destIndexNames = new string[0];
                var destTransformerNames = new string[0];
                var timeout = Debugger.IsAttached ? 60 * 1000000 : 3000;
                while (sw.ElapsedMilliseconds < timeout && destIndexNames.Length != 2)
                    destIndexNames = destination.Admin.Send(new GetIndexNamesOperation(0, 1024));

                sw.Restart();
                while (sw.ElapsedMilliseconds < timeout && destTransformerNames.Length != 2)
                    destTransformerNames = destination.Admin.Send(new GetTransformerNamesOperation(0, 1024));

                Assert.NotNull(destIndexNames); //precaution
                Assert.Equal(2, destIndexNames.Length);
                Assert.True(destIndexNames.Contains(userByAge.IndexName));
                Assert.True(destIndexNames.Contains(userByName.IndexName));

                Assert.NotNull(destTransformerNames); //precaution
                Assert.Equal(2, destTransformerNames.Length);
                Assert.True(destTransformerNames.Contains(usernameToUpperTransformer.TransformerName));
                Assert.True(destTransformerNames.Contains(usernameToLowerTransformer.TransformerName));
            }
        }

        [Fact]
        public async Task Can_replicate_transformer()
        {
            var (source, destination) = await CreateDuoCluster();

            using (source)
            using (destination)
            {
                await SetupReplicationAsync(source, destination);

                var usernameToUpperTransformer = new UsernameToUpperTransformer();
                usernameToUpperTransformer.Execute(source);

                var sw = Stopwatch.StartNew();
                var transformerNames = new string[0];
                var timeout = Debugger.IsAttached ? 60 * 1000000 : 3000;
                while (sw.ElapsedMilliseconds < timeout && transformerNames.Length != 1)
                    transformerNames = destination.Admin.Send(new GetTransformerNamesOperation(0, 1024));

                Assert.NotNull(transformerNames); //precaution
                Assert.Equal(1, transformerNames.Length);
                Assert.Equal(usernameToUpperTransformer.TransformerName, transformerNames.First());
            }
        }

        [Fact]
        public async Task Can_replicate_multiple_transformers()
        {
            var (source, destination) = await CreateDuoCluster();

            using (source)
            using (destination)
            {
                var usernameToUpperTransformer = new UsernameToUpperTransformer();
                usernameToUpperTransformer.Execute(source);

                var usernameToLowerTransformer = new UsernameToLowerTransformer();
                usernameToLowerTransformer.Execute(source);

                await SetupReplicationAsync(source, destination);

                var sw = Stopwatch.StartNew();
                var transformerNames = new string[0];
                var timeout = Debugger.IsAttached ? 60 * 1000000 : 3000;
                while (sw.ElapsedMilliseconds < timeout && transformerNames.Length != 2)
                    transformerNames = destination.Admin.Send(new GetTransformerNamesOperation(0, 1024));

                Assert.NotNull(transformerNames); //precaution
                Assert.Equal(2, transformerNames.Length);
                Assert.True(transformerNames.Contains(usernameToUpperTransformer.TransformerName));
                Assert.True(transformerNames.Contains(usernameToLowerTransformer.TransformerName));
            }
        }
       
       
        //An index can't be named with the same name as transformer or vice versa
        [Fact]
        public void Index_and_transformer_storage_should_enforce_name_uniqueness_for_writing_index_then_transformer()
        {
            using (var store = GetDocumentStore())
            {
                var userByNameIndex = new UserByNameIndex("FooBar");
                userByNameIndex.Execute(store);

                var usernameToUpperTransformer = new UsernameToUpperTransformer("FooBar");
                Assert.Throws<IndexOrTransformerAlreadyExistException>(() => usernameToUpperTransformer.Execute(store));
            }
        }

        [Fact]
        public void Index_and_transformer_storage_should_enforce_name_uniqueness_for_writing_transformer_then_index()
        {
            using (var store = GetDocumentStore())
            {
                var usernameToUpperTransformer = new UsernameToUpperTransformer("FooBar");
                usernameToUpperTransformer.Execute(store);

                var userByNameIndex = new UserByNameIndex("FooBar");
                Assert.Throws<IndexOrTransformerAlreadyExistException>(() => userByNameIndex.Execute(store));
            }
        }
       
    }
}




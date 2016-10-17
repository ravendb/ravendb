// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4977.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Core.Replication;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4977 : RavenReplicationCoreTest
    {
        [Fact]
        public void can_reset_index()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Users_By_Name();
                index.Execute(store);

                store.DatabaseCommands.Put("users/1", null, RavenJObject.FromObject(new User { Name = "Grisha" }), new RavenJObject());
                WaitForIndexing(store);

                var stats = store.DatabaseCommands.GetStatistics();
                Assert.Equal(2, stats.Indexes.Length);

                store.DatabaseCommands.ResetIndex(index.IndexName);
                WaitForIndexing(store);

                stats = store.DatabaseCommands.GetStatistics();
                Assert.Equal(2, stats.Indexes.Length);
            }
        }

        [Fact]
        public void can_reset_index_with_replication()
        {
            using (var store = GetDocumentStore())
            {
                SetupReplication(store, new List<RavenJObject> { new RavenJObject { { "Url", "http://localhost:8080" } } });
                var index = new Users_By_Name();
                index.Execute(store);

                store.DatabaseCommands.Put("users/1", null, RavenJObject.FromObject(new User { Name = "Grisha" }), new RavenJObject());
                WaitForIndexing(store);

                var stats = store.DatabaseCommands.GetStatistics();
                Assert.Equal(2, stats.Indexes.Length);

                store.DatabaseCommands.ResetIndex(index.IndexName);
                WaitForIndexing(store);

                stats = store.DatabaseCommands.GetStatistics();
                Assert.Equal(2, stats.Indexes.Length);
            }
        }

        public class User
        {
            public string Name { get; set; }

            public string Phone { get; set; }
        }

        public class Users_By_Name : AbstractIndexCreationTask<User>
        {
            public override string IndexName
            {
                get { return "test"; }
            }

            public Users_By_Name()
            {
                Map = users =>
                    from user in users
                    select new
                    {
                        user.Name
                    };
            }
        }

        public class Users_By_Name_And_Phone : AbstractIndexCreationTask<User>
        {
            public override string IndexName
            {
                get { return "test"; }
            }

            public Users_By_Name_And_Phone()
            {
                Map = users =>
                    from user in users
                    select new
                    {
                        user.Name,
                        user.Phone
                    };
            }
        }
    }
}

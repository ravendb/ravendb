// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4817.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.MailingList;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4817 : ReplicationBase
    {
        [Fact]
        public void ServerSideWriteAssurance()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();
            var store3 = CreateStore();

            SetupReplication(store1.DatabaseCommands, store2, store3);

            using (var s1 = store1.OpenSession())
            {
                s1.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2, timeout: TimeSpan.FromSeconds(5));

                s1.Store(new {Name = "Oren"}, "users/1");

                s1.SaveChanges();
            }

            using (var s2 = store2.OpenSession())
            {
                Assert.NotNull(s2.Load<dynamic>("users/1"));
            }

            using (var s3 = store3.OpenSession())
            {
                Assert.NotNull(s3.Load<dynamic>("users/1"));
            }
        }

        [Fact]
        public void ServerSideWriteAssuranceForSpecificIndex()
        {
            var store = CreateStore();
            store.ExecuteIndex(new UserByFirstName());
            using (var s = store.OpenSession())
            {
                s.Advanced.WaitForIndexesAfterSaveChanges(TimeSpan.FromSeconds(30),true,new [] { "UsersByName" });

                s.Store(new User { Name = "Oren" }, "users/1");

                s.SaveChanges();
                Assert.NotNull(s.Query<User>().Single(x => x.Name == "Oren"));
            }
        }

        class UsersByName : AbstractIndexCreationTask<User>
        {
            public UsersByName()
            {
                Map = docs => from doc in docs select new {doc.Name};
            }
        }

        class User
        {
            public string Name { get; set; }
        }

    }
}
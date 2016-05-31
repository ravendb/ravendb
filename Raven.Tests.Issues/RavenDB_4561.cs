using System;
using System.Linq;
using System.Net.Http;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Exceptions;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4561 : ReplicationBase
    {
        [Fact]
        public void CanProperlyMergeIncomingChangeWithConflictedDoc()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();

            TellFirstInstanceToReplicateToSecondInstance();

            // create doc on second node
            using (var session = store2.OpenSession())
            {
                session.Store(new Company { Name = "second_1" }, Etag.Empty, "companies/1");
                session.SaveChanges();
            }

            var etagAfterFirstSave = store2.DatabaseCommands.Get("companies/1").Etag;
            
            // create doc on first (it should create conflict)
            using (var session = store1.OpenSession())
            {
                session.Store(new Company { Name = "first_1" }, Etag.Empty, "companies/1");
                session.SaveChanges();
            }

            Etag etagWithFirstConflict = Etag.Empty;
            try
            {
                WaitForReplication(store2, "companies/1", changedSince: etagAfterFirstSave);
                Assert.False(true); // we expect conflict
            }
            catch (ConflictException e)
            {
                etagWithFirstConflict = e.Etag;
            }

            store2.DatabaseCommands.Put(Constants.RavenReplicationConfig, null, RavenJObject.FromObject(new ReplicationConfig
            {
                DocumentConflictResolution = StraightforwardConflictResolution.ResolveToLocal,
                AttachmentConflictResolution = StraightforwardConflictResolution.ResolveToLocal
            }), new RavenJObject());

            // create doc on first (it should update conflicted document)
            using (var session = store1.OpenSession())
            {
                session.Store(new Company { Name = "first_2" }, "companies/1");
                session.SaveChanges();
            }

            for (int i = 0; i < RetriesCount; i++)
            {
                try
                {
                    store2.DatabaseCommands.Get("companies/1");
                }
                catch (ConflictException e)
                {
                    if (e.Etag != etagWithFirstConflict)
                        break;
                }
            }

            using (var requst = store2.DatabaseCommands.CreateRequest("/studio-tasks/resolveMerge?documentId=companies%2F1", HttpMethod.Get))
            {
                var response = requst.ReadResponseJson().ToString();
                Assert.Contains("/*<<<", response); // conflict
                Assert.Contains("first_2", response); // latest first
                Assert.Contains("second_1", response); // latest second
            }
        }
    }
}
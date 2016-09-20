using System;
using System.Collections.Generic;
using System.Net;
using Raven.Abstractions.Connection;
using Raven.Client.Data;
using Raven.Client.Exceptions;
using Xunit;

namespace FastTests.Server.Documents.Replication
{
    public class ManualConflictResolution : ReplicationTestsBase
    {
        [Fact(Skip = "This needs to be refactored after ClientAPI will support the new conflict semantics")]
        public void CanManuallyResolveConflict()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                SetupReplication(master, slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "local"
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    session.Store(new ReplicationConflictsTests.User()
                    {
                        Name = "remote"
                    }, "users/1");
                    session.SaveChanges();
                }

                System.Threading.Thread.Sleep(2000);

                using (var session = slave.OpenSession())
                {
                    try
                    {
                        var item = session.Load<ReplicationConflictsTests.User>("users/1");
                    }
                    catch (ErrorResponseException e)
                    {
                        Assert.Equal(HttpStatusCode.Conflict, e.StatusCode);
                        //var list = new List<JsonDocument>();
                        //for (int i = 0; i < e.ConflictedVersionIds.Length; i++)
                        //{
                        //	var doc = slave.DatabaseCommands.Get(e.ConflictedVersionIds[i]);
                        //	list.Add(doc);
                        //}

                        //var resolved = list[0];
                        ////TODO : when client API is finished, refactor this so the test works as designed
                        ////resolved.Metadata.Remove(Constants.RavenReplicationConflictDocument);
                        //slave.DatabaseCommands.Put("users/1", null, resolved.DataAsJson, resolved.Metadata);
                    }
                }


            }
        }
    }
}

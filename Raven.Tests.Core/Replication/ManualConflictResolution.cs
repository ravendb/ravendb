using Raven.Abstractions.Data;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Core.Replication
{
    public class ManualConflictResolution : RavenReplicationCoreTest
    {
#if DNXCORE50
        public ManualConflictResolution(TestServerFixture fixture)
            : base(fixture)
        {

        }
#endif

        [Fact]
        public void CanManuallyResolveConflict()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                SetupReplication(master, destinations: slave);

                using (var session = slave.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "local"
                    }, "users/1");
                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    session.Store(new User()
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
                        var item = session.Load<User>("users/1");
                    }
                    catch (ConflictException e)
                    {
                        var list = new List<JsonDocument>();
                        for (int i = 0; i < e.ConflictedVersionIds.Length; i++)
                        {
                            var doc = slave.DatabaseCommands.Get(e.ConflictedVersionIds[i]);
                            list.Add(doc);
                        }

                        var resolved = list[0];
                        resolved.Metadata.Remove(Constants.RavenReplicationConflictDocument);
                        slave.DatabaseCommands.Put("users/1", null, resolved.DataAsJson, resolved.Metadata);
                    }
                }


            }
        }
    }
}

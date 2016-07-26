using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Json.Linq;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;
using Constants = Raven.Abstractions.Data.Constants;
namespace Raven.Tests.Conflicts
{
    public class ShouldntCreateConflictIfIdentical : ReplicationBase
    {
        private static RavenJObject userAsObject = RavenJObject.FromObject(
            new { Name = "Tal", Address = new { Country = "Israel", City = "Zichron" } });

        private static byte[] data = {13, 35, 65};

        [Fact]
        public void IdenticalDocumentsShouldntCreateConflicts()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            {
                store1.DatabaseCommands.Put("users/1", null, userAsObject, null);
                store2.DatabaseCommands.Put("users/1", null, userAsObject, null);
                var userOnStore1 = store1.DatabaseCommands.Get("users/1");
                var user = store2.DatabaseCommands.Get("users/1");
                TellFirstInstanceToReplicateToSecondInstance();
                WaitForReplication(store2, "users/1", changedSince: user.Etag);
                //if this line doesn't throw this means that the conflict was resolved.
                user = store2.DatabaseCommands.Get("users/1");
                //Making sure user on store 2 has the history of versions from store 1
                Assert.Equal(1, ((RavenJArray)user.Metadata[Constants.RavenReplicationHistory]).Length);
                Assert.Equal(userOnStore1.Metadata[Constants.RavenReplicationSource].Value<string>(),
                    ((RavenJObject)((RavenJArray)user.Metadata[Constants.RavenReplicationHistory]).First())[Constants.RavenReplicationSource].Value<string>());
            }             
        }

        [Fact]
        public void IdenticalAttachmentsShouldntCreateConflicts()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            {
                store1.DatabaseCommands.PutAttachment("attachments/1", null, new MemoryStream(data), null);
                store2.DatabaseCommands.PutAttachment("attachments/1", null, new MemoryStream(data), null);
                var attOnStore1 = store1.DatabaseCommands.GetAttachment("attachments/1");
                var att = store2.DatabaseCommands.GetAttachment("attachments/1");
                TellFirstInstanceToReplicateToSecondInstance();
                WaitForAttachment(store2, "attachments/1", att.Etag);
                //if this line doesn't throw this means that the conflict was resolved.
                att = store2.DatabaseCommands.GetAttachment("attachments/1");
                //Making sure user on store 2 has the history of versions from store 1
                Assert.Equal(1, ((RavenJArray)att.Metadata[Constants.RavenReplicationHistory]).Length);
                Assert.Equal(attOnStore1.Metadata[Constants.RavenReplicationSource].Value<string>(),
                    ((RavenJObject)((RavenJArray)att.Metadata[Constants.RavenReplicationHistory]).First())[Constants.RavenReplicationSource].Value<string>());
            }
        }
    }
}

using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Xunit;

namespace Raven.Bundles.Tests.Replication
{
    public class AttachmentReplication : ReplicationBase
    {
        [Fact]
        public void Can_replicate_between_two_instances()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();

            TellFirstInstanceToReplicateToSecondInstance();

            store1.DatabaseCommands.PutAttachment("ayende", null, new byte[] {1, 2, 3}, new JObject());


            Attachment attachment = null;
            for (int i = 0; i < RetriesCount; i++)
            {
                attachment = store2.DatabaseCommands.GetAttachment("ayende");
                if(attachment == null)
                    Thread.Sleep(100);
            }
            Assert.NotNull(attachment);
            Assert.Equal(new byte[]{1,2,3}, attachment.Data);

        }
    }
}
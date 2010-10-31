using System.ComponentModel.Composition.Hosting;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Raven.Tests.Storage;
using System.Linq;
using Xunit;

namespace Raven.Tests.Triggers
{
    public class AttachmentReadTrigger : AbstractDocumentStorageTest
    {
        private readonly DocumentDatabase db;

        public AttachmentReadTrigger()
        {
            db = new DocumentDatabase(new RavenConfiguration
            {
                DataDirectory = "raven.db.test.esent",
                Container = new CompositionContainer(new TypeCatalog(
                    typeof(HideAttachmnetByCaseReadTrigger)))
            });

        }

        public override void Dispose()
        {
            db.Dispose();
            base.Dispose();
        }

        [Fact]
        public void CanFilterAttachment()
        {
            db.PutStatic("ayendE", null, new byte[] { 1, 2 }, new JObject());

            var attachment = db.GetStatic("ayendE");

            Assert.Equal("You don't get to read this attachment",
                         attachment.Metadata.Value<JObject>("Raven-Read-Veto").Value<string>("Reason"));
        }

        [Fact]
        public void CanHideAttachment()
        {
            db.PutStatic("AYENDE", null, new byte[] { 1, 2 }, new JObject());

            var attachment = db.GetStatic("AYENDE");

            Assert.Null(attachment);
        }

        [Fact]
        public void CanModifyAttachment()
        {
            db.PutStatic("ayende", null, new byte[] { 1, 2 }, new JObject());


            var attachment = db.GetStatic("ayende");

            Assert.Equal(attachment.Data.Length, 4);
        }

        public class HideAttachmnetByCaseReadTrigger : AbstractAttachmentReadTrigger
        {
            public override ReadVetoResult AllowRead(string key, byte[] data, Newtonsoft.Json.Linq.JObject metadata, ReadOperation operation)
            {
                if (key.All(char.IsUpper))
                    return ReadVetoResult.Ignore;
                if (key.Any(char.IsUpper))
                    return ReadVetoResult.Deny("You don't get to read this attachment");
                return ReadVetoResult.Allowed;
            }

            public override byte[] OnRead(string key, byte[] data, JObject metadata, ReadOperation operation)
            {
                return new byte[] { 1, 2, 3, 4 };
            }
        }
    }
}
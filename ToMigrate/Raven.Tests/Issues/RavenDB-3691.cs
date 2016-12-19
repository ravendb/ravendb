using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3691 : RavenTestBase
    {
        [Fact]
        public void CanPutDocumentWithMetadataPropertyBeingNull()
        {
            using (var server = GetNewServer())
            {
                using (var documentStore = new DocumentStore { Url = server.SystemDatabase.Configuration.ServerUrl }.Initialize())
                {
                    documentStore.DatabaseCommands.Put("test", null, new RavenJObject(), RavenJObject.FromObject(new { Foo = (string)null }));
                }
            }
        }

        [Fact]
        public void CanPutAttachmentWithMetadataPropertyBeingNull()
        {
            using (var server = GetNewServer())
            {
                using (var documentStore = new DocumentStore { Url = server.SystemDatabase.Configuration.ServerUrl }.Initialize())
                {
                    documentStore.DatabaseCommands.PutAttachment("test", null, new MemoryStream(new byte[] { 1, 2, 3, 4 }), RavenJObject.FromObject(new { Foo = (string)null }));
                }
            }
        }
    }
}

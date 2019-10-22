using System;
using System.IO;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_11379 : EtlTestBase
    {
        public RavenDB_11379(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(null, false, "photo", "photo")]
        [InlineData(null, true, "photo", "photo")]
        [InlineData(@"

var doc = loadToUsers(this);

doc.addAttachment(loadAttachment('photo'));

", false, "photo", "photo")]
        [InlineData(@"

var doc = loadToUsers(this);

doc.addAttachment('image', loadAttachment('photo'));

", false, "photo", "image")]
        [InlineData(@"

var doc = loadToUsers(this);

var attachments = this['@metadata']['@attachments'];

for (var i = 0; i < attachments.length; i++) {
    if (attachments[i].Name.endsWith('.png'))
        doc.addAttachment(loadAttachment(attachments[i].Name));
}
", false, "photo.png", "photo.png")]
        public void Should_remove_attachment(string script, bool applyToAllDocuments, string attachmentSourceName, string attachmentDestinationName)
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                if (applyToAllDocuments)
                    AddEtl(src, dest, new string[0], script: null, applyToAllDocuments: true);
                else
                    AddEtl(src, dest, "Users", script: script);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");

                    session.Advanced.Attachments.Store("users/1", attachmentSourceName, new MemoryStream(new byte[] { 1 }));

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    var attachment = session.Advanced.Attachments.Get("users/1", attachmentDestinationName);

                    Assert.NotNull(attachment);
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Advanced.Attachments.Delete("users/1", attachmentSourceName);

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    var attachment = session.Advanced.Attachments.Get("users/1", attachmentDestinationName);

                    Assert.Null(attachment);
                }
            }
        }
    }
}

using System;
using System.IO;
using Raven.Server.Config;
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
                    using (var attachment = session.Advanced.Attachments.Get("users/1", attachmentDestinationName))
                    {
                        Assert.NotNull(attachment);
                    }
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
                    using (var attachment = session.Advanced.Attachments.Get("users/1", attachmentDestinationName))
                    {
                        Assert.Null(attachment);
                    }
                }
            }
        }

        [Fact]
        public void Should_remove_attachment2()
        {
            var attachmentSourceName = "photo";
            var attachmentDestinationName = "photo";
            using (var src = GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = x =>
                {
                    x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedDocuments)] = "1";
                    x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedItems)] = "1";
                }
            }))
            using (var dest = GetDocumentStore())
            {
                var numberOfDocs = 10;

                for (int i = 0; i < numberOfDocs; i++)
                {
                    using (var session = src.OpenSession())
                    {
                    
                        session.Store(new User(), "users/" + i);

                        session.Advanced.Attachments.Store("users/" + i, attachmentSourceName, new MemoryStream(new byte[] { (byte)i }));

                        session.SaveChanges();
                    }   
                }

                AddEtl(src, dest, new string[0], script: null, applyToAllDocuments: true);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses >= 2 * numberOfDocs);

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));
                
                etlDone.Reset();

                for (int i = 0; i < numberOfDocs; i++)
                {
                    using (var session = dest.OpenSession())
                    {
                        using (var attachment = session.Advanced.Attachments.Get("users/" + i, attachmentDestinationName))
                        {
                            Assert.NotNull(attachment);
                        }
                    }
                }

                for (int i = 0; i < numberOfDocs; i++)
                {
                    using (var session = src.OpenSession())
                    {
                        session.Advanced.Attachments.Delete("users/"+i, attachmentSourceName);

                        session.SaveChanges();
                    }
                }
                WaitForUserToContinueTheTest(dest);
                var etlDone2 = WaitForEtl(src, (n, s) => s.LoadSuccesses >= numberOfDocs * 4);
                Assert.True(etlDone2.Wait(TimeSpan.FromMinutes(1)));

                etlDone.Reset();

                for (int i = 0; i < numberOfDocs; i++)
                {
                    using (var session = dest.OpenSession())
                    {
                        using (var attachment = session.Advanced.Attachments.Get("users/" + i, attachmentDestinationName))
                        {
                            Assert.Null(attachment);
                        }
                    }
                }
            }
        }

        [Fact]
        public void Should_send_tombstones()
        {
            using (var src = GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = x =>
                {
                    x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedDocuments)] = "1";
                    x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedItems)] = "1";
                }
            }))
            using (var dest = GetDocumentStore())
            {
                var numberOfDocs = 10;

                for (int i = 0; i < numberOfDocs; i++)
                {
                    using (var session = src.OpenSession())
                    {
                        session.Store(new User(), "users/" + i);
                        session.Store(new Company(), "companies/" + i);

                        session.SaveChanges();
                    }   
                }

                AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses >= numberOfDocs);

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));
                
                etlDone.Reset();

                for (int i = 0; i < numberOfDocs; i++)
                {
                    using (var session = dest.OpenSession())
                    {
                        var user = session.Load<User>("users/" + i);

                        Assert.NotNull(user);
                    }
                }
                
                using (var session = src.OpenSession())
                {
                    for (int i = 0; i < numberOfDocs; i++)
                    {
                        session.Delete("companies/" + i);
                    }

                    for (int i = 0; i < numberOfDocs; i++)
                    {
                        session.Delete("users/" + i);
                    }

                    session.SaveChanges();
                }
                WaitForUserToContinueTheTest(dest);

                var etlDone2 = WaitForEtl(src, (n, s) => s.LoadSuccesses >= numberOfDocs * 2);
                Assert.True(etlDone2.Wait(TimeSpan.FromMinutes(1)));

                for (int i = 0; i < numberOfDocs; i++)
                {
                    using (var session = dest.OpenSession())
                    {
                        var user = session.Load<User>("users/" + i);
                        Assert.Null(user);
                    }
                }
            }
        }
    }
}

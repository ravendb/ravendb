using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Attachments
{
    public class AttachmentsBigFiles : RavenTestBase
    {
        public AttachmentsBigFiles(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(10, "i1enlqXQfLBMwWFN/CrLP3PtxxLX9DNhnKO75muxX0k=", false)]
        [InlineData(10, "i1enlqXQfLBMwWFN/CrLP3PtxxLX9DNhnKO75muxX0k=", true)]
        public async Task BatchRequestWithLongMultiPartSections(long size, string hash, bool encrypted)
        {
            string dbName = null;
            X509Certificate2 adminCert = null;
            if (encrypted)
            {
                var backupPath = NewDataPath(suffix: "BackupFolder");
                var key = Encryption.EncryptedServer(out var certificates, out dbName);
                adminCert = certificates.ServerCertificate.Value;
            }
            else
            {
                dbName = GetDatabaseName();
            }

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record => record.Encrypted = encrypted,
                Path = NewDataPath()
            }))
            {
                var dbId1 = new Guid("00000000-48c4-421e-9466-000000000000");
                await Databases.SetDatabaseId(store, dbId1);

                using (var session = store.OpenSession())
                using (var stream = new BigDummyStream(size))
                {
                    var user = new User { Name = "Fitzchak" };
                    session.Store(user, "users/1");

                    session.Advanced.Attachments.Store(user, "big-file", stream);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    using (var bigStream = new BigDummyStream(size))
                    using (var attachment = session.Advanced.Attachments.Get(user, "big-file"))
                    {
                        Assert.Contains("A:2", attachment.Details.ChangeVector);
                        Assert.Equal("big-file", attachment.Details.Name);
                        Assert.Equal(hash, attachment.Details.Hash);
                        Assert.Equal(size, attachment.Details.Size);
                        Assert.Equal("", attachment.Details.ContentType);

                        attachment.Stream.CopyTo(bigStream);
                        Assert.Equal(size, bigStream.Position);
                    }
                }
            }
        }

        [Theory]
        [InlineData(10, "i1enlqXQfLBMwWFN/CrLP3PtxxLX9DNhnKO75muxX0k=", false)]
        [InlineData(10, "i1enlqXQfLBMwWFN/CrLP3PtxxLX9DNhnKO75muxX0k=", true)]
        public async Task SupportHugeAttachment(long size, string hash, bool encrypted)
        {
            string dbName = null;
            X509Certificate2 adminCert = null;
            if (encrypted)
            {
                var backupPath = NewDataPath(suffix: "BackupFolder");
                var key = Encryption.EncryptedServer(out var certificates, out dbName);
                adminCert = certificates.ServerCertificate.Value;
            }
            else
            {
                dbName = GetDatabaseName();
            }

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record => record.Encrypted = encrypted,
                Path = NewDataPath()
            }))
            {
                var dbId1 = new Guid("00000000-48c4-421e-9466-000000000000");
                await Databases.SetDatabaseId(store, dbId1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Fitzchak" }, "users/1");
                    session.SaveChanges();
                }

                using (var bigStream = new BigDummyStream(size))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation("users/1", "huge-file", bigStream));
                    Assert.Contains("A:2", result.ChangeVector);
                    Assert.Equal("huge-file", result.Name);
                    Assert.Equal("users/1", result.DocumentId);
                    Assert.Equal("", result.ContentType);
                    Assert.Equal(hash, result.Hash);
                    Assert.Equal(size, result.Size);
                    Assert.Equal(size, bigStream.Position);
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    using (var bigStream = new BigDummyStream(size))
                    using (var attachment = session.Advanced.Attachments.Get(user, "huge-file"))
                    {
                        attachment.Stream.CopyTo(bigStream);
                        Assert.Contains("A:2", attachment.Details.ChangeVector);
                        Assert.Equal("huge-file", attachment.Details.Name);
                        Assert.Equal(hash, attachment.Details.Hash);
                        Assert.Equal(size, bigStream.Position);
                        Assert.Equal(size, attachment.Details.Size);
                        Assert.Equal("", attachment.Details.ContentType);
                    }
                }
            }
        }
    }
}

using Tests.Infrastructure;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Encryption
{
    public class ShardedEncryption : RavenTestBase
    {
        public ShardedEncryption(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Encryption | RavenTestCategory.Sharding, LicenseRequired = true)]
        public void Can_Setup_Sharded_Encrypted_Database()
        {
            Encryption.EncryptedServer(out var certificates, out var dbName);

            var options = new Options
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseRecord = record =>
                {
                    record.Encrypted = true;
                },
                ModifyDatabaseName = s => dbName
            };


            using (var store = Sharding.GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "ayende"
                    }, "users/1");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>("users/1");
                    Assert.Equal("ayende", loaded.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Encryption | RavenTestCategory.Sharding, LicenseRequired = true)]
        public async Task CRUD_Operations_Encrypted()
        {
            Encryption.EncryptedServer(out var certificates, out var dbName);

            var options = new Options
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseRecord = record =>
                {
                    record.Encrypted = true;
                },
                ModifyDatabaseName = s => dbName
            };

            using (var store = Sharding.GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "user1" }, "users/1");
                    var user2 = new User { Name = "user2", Age = 1 };
                    session.Store(user2, "users/2");
                    var user3 = new User { Name = "user3", Age = 1 };
                    session.Store(user3, "users/3");
                    session.Store(new User { Name = "user4" }, "users/4");

                    session.Delete(user2);
                    user3.Age = 3;
                    session.SaveChanges();

                    var tempUser = session.Load<User>("users/2");
                    Assert.Null(tempUser);
                    tempUser = session.Load<User>("users/3");
                    Assert.Equal(tempUser.Age, 3);
                    var user1 = session.Load<User>("users/1");
                    var user4 = session.Load<User>("users/4");

                    session.Delete(user4);
                    user1.Age = 10;
                    session.SaveChanges();

                    tempUser = session.Load<User>("users/4");
                    Assert.Null(tempUser);
                    tempUser = session.Load<User>("users/1");
                    Assert.Equal(tempUser.Age, 10);
                }

                using (var session = store.OpenAsyncSession())
                {
                    await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                    {
                        session.Advanced.Attachments.Store("users/1", "profile.png", profileStream, "image/png");
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal(DocumentFlags.HasAttachments.ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(1, attachments.Length);
                    var attachment = attachments[0];
                    Assert.Equal("profile.png", attachment.GetString(nameof(AttachmentName.Name)));
                    var hash = attachment.GetString(nameof(AttachmentName.Hash));
                    Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                    Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                }
            }
        }
    }
}

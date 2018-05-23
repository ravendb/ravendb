using System;
using System.IO;
using FastTests.Voron.Util;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_7064 : BasicRavenEtlTests
    {
        [Fact]
        public void Should_handle_attachments()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script:
                    @"
var attachments = this['@metadata']['@attachments'];

this.Name = 'James';

// case 1 : doc id will be preserved

for (var i = 0; i < attachments.length; i++) {
    addAttachment(this, attachments[i].Name);
}

loadToUsers(this);

// case 2 : doc id will be generated on the destination side

var person = { Name: this.Name + ' ' + this.LastName };

addAttachment(person, 'photo2.jpg');

loadToPeople(person);
"                   
);
                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe",
                        LastName = "Doe",
                    }, "users/1");

                    session.Advanced.Attachments.Store("users/1", "photo1.jpg", new MemoryStream(new byte[] {1}));
                    session.Advanced.Attachments.Store("users/1", "photo2.jpg", new MemoryStream(new byte[] {2}));

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                AssertAttachments(dest, new []
                {
                    ("users/1", "photo1.jpg", new byte[] {1}, false),
                    ("users/1", "photo2.jpg", new byte[] {2}, false),
                    ("users/1/people/", "photo2.jpg", new byte[] {2}, true)
                });

                string personId;

                using (var session = dest.OpenSession())
                {
                    personId = session.Advanced.LoadStartingWith<Person>("users/1/people/")[0].Id;
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Advanced.Attachments.Delete("users/1", "photo1.jpg");
                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    var metatata = session.Advanced.GetMetadataFor(user);

                    Assert.True(metatata.ContainsKey(Constants.Documents.Metadata.Attachments));

                    var attachment = session.Advanced.Attachments.Get("users/1", "photo1.jpg");

                    Assert.Null(attachment); // this attachment was removed
                }

                AssertAttachments(dest, new []
                {
                    ("users/1", "photo2.jpg", new byte[] {2}, false),
                    ("users/1/people/", "photo2.jpg", new byte[] {2}, true)
                });

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Null(session.Load<User>("users/1"));

                    Assert.Null(session.Advanced.Attachments.Get("users/1", "photo1.jpg"));
                    Assert.Null(session.Advanced.Attachments.Get("users/1", "photo2.jpg"));

                    Assert.Empty(session.Advanced.LoadStartingWith<Person>("users/1/people/"));

                    Assert.Null(session.Advanced.Attachments.Get(personId, "photo2.jpg"));
                }
            }
        }

        private void AssertAttachments(IDocumentStore store, params (string DocId, string AttachmentName, byte[] AttachmentData, bool LoadUsingStartingWith)[] items)
        {
            using (var session = store.OpenSession())
            {
                foreach (var item in items)
                {
                    var doc = item.LoadUsingStartingWith ? session.Advanced.LoadStartingWith<User>(item.DocId)[0] : session.Load<User>(item.DocId);
                    Assert.NotNull(doc);

                    var metatata = session.Advanced.GetMetadataFor(doc);

                    Assert.True(metatata.ContainsKey(Constants.Documents.Metadata.Attachments));


                    var attachment = session.Advanced.Attachments.Get(doc.Id, item.AttachmentName);

                    Assert.NotNull(attachment);
                    Assert.Equal(item.AttachmentName, attachment.Details.Name);
                    Assert.Equal(item.AttachmentData, attachment.Stream.ReadData());
                }
            }
        }
    }
}

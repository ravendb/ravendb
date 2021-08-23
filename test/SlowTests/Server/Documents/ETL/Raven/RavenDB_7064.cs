using System;
using System.IO;
using FastTests.Voron.Util;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_7064 : EtlTestBase
    {
        public RavenDB_7064(ITestOutputHelper output) : base(output)
        {
        }

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

var doc = loadToUsers(this);

for (var i = 0; i < attachments.length; i++) {
    doc.addAttachment(attachments[i].Name + '-etl', loadAttachment(attachments[i].Name));
}

// case 2 : doc id will be generated on the destination side

var person = loadToPeople({ Name: this.Name + ' ' + this.LastName });

person.addAttachment('photo2.jpg-etl', loadAttachment('photo2.jpg'));
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

                    session.Advanced.Attachments.Store("users/1", "photo1.jpg", new MemoryStream(new byte[] { 1 }));
                    session.Advanced.Attachments.Store("users/1", "photo2.jpg", new MemoryStream(new byte[] { 2 }));

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                AssertAttachments(dest, new[]
                {
                    ("users/1", "photo1.jpg-etl", new byte[] {1}, false),
                    ("users/1", "photo2.jpg-etl", new byte[] {2}, false),
                    ("users/1/people/", "photo2.jpg-etl", new byte[] {2}, true)
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

                    var metadata = session.Advanced.GetMetadataFor(user);

                    Assert.True(metadata.ContainsKey(Constants.Documents.Metadata.Attachments));

                    using (var attachment = session.Advanced.Attachments.Get("users/1", "photo1.jpg-etl"))
                    {
                        Assert.Null(attachment); // this attachment was removed
                    }
                }

                AssertAttachments(dest, new[]
                {
                    ("users/1", "photo2.jpg-etl", new byte[] {2}, false),
                    ("users/1/people/", "photo2.jpg-etl", new byte[] {2}, true)
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

                    using (var attachment = session.Advanced.Attachments.Get("users/1", "photo1.jpg-etl"))
                    {
                        Assert.Null(attachment);
                    }

                    using (var attachment = session.Advanced.Attachments.Get("users/1", "photo2.jpg-etl"))
                    {
                        Assert.Null(attachment);
                    }

                    Assert.Empty(session.Advanced.LoadStartingWith<Person>("users/1/people/"));

                    using (var attachment = session.Advanced.Attachments.Get(personId, "photo2.jpg-etl"))
                    {
                        Assert.Null(attachment);
                    }
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

                    var metadata = session.Advanced.GetMetadataFor(doc);

                    Assert.True(metadata.ContainsKey(Constants.Documents.Metadata.Attachments));


                    using (var attachment = session.Advanced.Attachments.Get(doc.Id, item.AttachmentName))
                    {
                        Assert.NotNull(attachment);
                        Assert.Equal(item.AttachmentName, attachment.Details.Name);
                        Assert.Equal(item.AttachmentData, attachment.Stream.ReadData());
                    }
                }
            }
        }

        [Fact]
        public void Can_use_get_attachments()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script:
                    @"
var attachments = getAttachments();

for (var i = 0; i < attachments.length; i++) {
    this.LastName = this.LastName + attachments[i].Name;
}

var doc = loadToUsers(this);

for (var i = 0; i < attachments.length; i++) {
    doc.addAttachment(loadAttachment(attachments[i].Name));
}
"
);
                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe",
                        LastName = "",
                    }, "users/1");

                    session.Advanced.Attachments.Store("users/1", "photo1.jpg", new MemoryStream(new byte[] { 1 }));
                    session.Advanced.Attachments.Store("users/1", "photo2.jpg", new MemoryStream(new byte[] { 2 }));

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                AssertAttachments(dest, new[]
                {
                    ("users/1", "photo1.jpg", new byte[] {1}, false),
                    ("users/1", "photo2.jpg", new byte[] {2}, false),
                });

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.Equal("photo1.jpgphoto2.jpg", user.LastName);
                }
            }
        }

        [Fact]
        public void Can_use_has_attachment()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script:
                    @"

var doc = loadToUsers(this);

if (hasAttachment('photo1.jpg')) {
  doc.addAttachment(loadAttachment('photo1.jpg'));
}

if (hasAttachment('photo2.jpg')) {
  doc.addAttachment(loadAttachment('photo2.jpg'));
}

"
                );
                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe"
                    }, "users/1");

                    session.Advanced.Attachments.Store("users/1", "photo2.jpg", new MemoryStream(new byte[] { 1 }));

                    session.Store(new User()
                    {
                        Name = "Joe"
                    }, "users/2");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                AssertAttachments(dest, new[]
                {
                    ("users/1", "photo2.jpg", new byte[] {1}, false)
                });

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.NotNull(session.Load<User>("users/2"));
                }
            }
        }
    }
}

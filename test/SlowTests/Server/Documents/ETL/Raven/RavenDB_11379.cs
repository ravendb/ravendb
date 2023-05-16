using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static SlowTests.Issues.RavenDB_13335;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_11379 : RavenTestBase
    {
        public RavenDB_11379(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(null, false, "photo", "photo", DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(null, true, "photo", "photo", DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(@"

var doc = loadToUsers(this);

doc.addAttachment(loadAttachment('photo'));

", false, "photo", "photo", DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(@"

var doc = loadToUsers(this);

doc.addAttachment('image', loadAttachment('photo'));

", false, "photo", "image", DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(@"

var doc = loadToUsers(this);

var attachments = this['@metadata']['@attachments'];

for (var i = 0; i < attachments.length; i++) {
    if (attachments[i].Name.endsWith('.png'))
        doc.addAttachment(loadAttachment(attachments[i].Name));
}
", false, "photo.png", "photo.png", DatabaseMode = RavenDatabaseMode.All)]

        public void Should_remove_attachment(Options options, string script, bool applyToAllDocuments, string attachmentSourceName, string attachmentDestinationName)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                if (applyToAllDocuments)
                    Etl.AddEtl(src, dest, new string[0], script: null, applyToAllDocuments: true);
                else
                    Etl.AddEtl(src, dest, "Users", script: script);

                var etlDone = Etl.WaitForEtlToComplete(src);

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

        private static async Task<bool> WaitForETlAsync(Action act, long timeout = 30_000)
        {
            /*if (Debugger.IsAttached)
                timeout *= 100;*/

            var sw = Stopwatch.StartNew();
            while (true)
            {
                try
                {
                    act.Invoke();
                    return true;
                }
                catch
                {
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        throw;
                    }
                }
                await Task.Delay(100);
            }
        }



        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Should_remove_attachment2(Options options)
        {
            var attachmentSourceName = "photo";
            var attachmentDestinationName = "photo";

            options.ModifyDatabaseRecord += x =>
            {
                x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedDocuments)] = "1";
                x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedItems)] = "1";
            };

            using (var src = GetDocumentStore(options))
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

                Etl.AddEtl(src, dest, new string[0], script: null, applyToAllDocuments: true);

                await Etl.AssertEtlReachedDestination(() =>
                {
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
                });

                for (int i = 0; i < numberOfDocs; i++)
                {
                    using (var session = src.OpenSession())
                    {
                        session.Advanced.Attachments.Delete("users/" + i, attachmentSourceName);

                        session.SaveChanges();
                    }
                }

                await Etl.AssertEtlReachedDestination(() =>
                {
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
                });
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Should_send_tombstones(Options options)
        {
            options.ModifyDatabaseRecord += x =>
            {
                x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedDocuments)] = "1";
                x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedItems)] = "1";
            };
            using (var src = GetDocumentStore(options))
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

                Etl.AddEtl(src, dest, "Users", script: null);

                await Etl.AssertEtlReachedDestination(() =>
                {
                    for (int i = 0; i < numberOfDocs; i++)
                    {
                        using (var session = dest.OpenSession())
                        {
                            var user = session.Load<User>("users/" + i);

                            Assert.NotNull(user);
                        }
                    }
                });

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

                await Etl.AssertEtlReachedDestination(() =>
                {
                    for (int i = 0; i < numberOfDocs; i++)
                    {
                        using (var session = dest.OpenSession())
                        {
                            var user = session.Load<User>("users/" + i);
                            Assert.Null(user);
                        }
                    }
                });
            }
        }
    }
}

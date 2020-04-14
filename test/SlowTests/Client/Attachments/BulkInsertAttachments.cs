using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Attachments
{
    public class BulkInsertAttachments : RavenTestBase
    {
        public BulkInsertAttachments(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(1, 32 * 1024)]
        [InlineData(100, 1 * 1024 * 1024)]
        [InlineData(100, 256 * 1024)]
        [InlineData(200, 128 * 1024)]
        [InlineData(1000, 16 * 1024)]
        public async Task StoreManyAttachments(int count, int size)
        {
            using (var store = GetDocumentStore())
            {
                const string userId = "user/1";
                var streams = new Dictionary<string, MemoryStream>();
                using (var bulkInsert = store.BulkInsert())
                {
                    var user1 = new User { Name = "EGR" };
                    bulkInsert.Store(user1, userId);
                    var attachmentsBulkInsert = bulkInsert.AttachmentsFor(userId);
                    for (int i = 0; i < count; i++)
                    {
                        var rnd = new Random(DateTime.Now.Millisecond);
                        var bArr = new byte[size];
                        rnd.NextBytes(bArr);
                        var name = i.ToString();
                        var stream = new MemoryStream(bArr);

                        await attachmentsBulkInsert.StoreAsync(name, stream);

                        stream.Position = 0;
                        streams[name] = stream;
                    }
                }

                using (var session = store.OpenSession())
                {
                    var attachmentsNames = streams.Select(x => new AttachmentRequest(userId, x.Key));
                    var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);

                    while (attachmentsEnumerator.MoveNext())
                    {
                        Assert.NotNull(attachmentsEnumerator.Current != null);
                        Assert.True(AttachmentsStreamTests.CompareStreams(attachmentsEnumerator.Current.Stream, streams[attachmentsEnumerator.Current.Details.Name]));
                    }
                }
            }
        }

        [Theory]
        [InlineData(100, 100, 16 * 1024)]
        [InlineData(250, 50, 64 * 1024)]
        public async Task StoreManyAttachmentsAndDocs(int count, int attachments, int size)
        {
            using (var store = GetDocumentStore())
            {
                var streams = new Dictionary<string, Dictionary<string, MemoryStream>>();
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < count; i++)
                    {
                        var id = $"user/{i}";
                        streams[id] = new Dictionary<string, MemoryStream>();
                        bulkInsert.Store(new User { Name = $"EGR_{i}" }, id);
                        var attachmentsBulkInsert = bulkInsert.AttachmentsFor(id);
                        for (int j = 0; j < attachments; j++)
                        {
                            var rnd = new Random(DateTime.Now.Millisecond);
                            var bArr = new byte[size];
                            rnd.NextBytes(bArr);
                            var name = j.ToString();
                            var stream = new MemoryStream(bArr);
                            await attachmentsBulkInsert.StoreAsync(name, stream);

                            stream.Position = 0;
                            streams[id][name] = stream;
                        }
                    }
                }

                foreach (var id in streams.Keys)
                {
                    using (var session = store.OpenSession())
                    {
                        var attachmentsNames = streams.Select(x => new AttachmentRequest(id, x.Key));
                        var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);

                        while (attachmentsEnumerator.MoveNext())
                        {
                            Assert.NotNull(attachmentsEnumerator.Current != null);
                            Assert.True(AttachmentsStreamTests.CompareStreams(attachmentsEnumerator.Current.Stream, streams[id][attachmentsEnumerator.Current.Details.Name]));
                        }
                    }
                }
            }
        }
    }
}

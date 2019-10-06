using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Attachments
{
    public class AttachmentsStreamTests : RavenTestBase
    {
        [Theory]
        [InlineData(1024)]
        [InlineData(32 * 1024)]
        [InlineData(256 * 1024)]
        [InlineData(1024 * 1024)]
        [InlineData(128 * 1024 * 1024)]
        [InlineData(1024 * 1024 * 1024)]
        public void CanGetOneAttachment(int size)
        {
            using (var stream = new MemoryStream(Enumerable.Repeat((byte)0x20, size).ToArray()))
            {
                const string id = "users/1";
                const string attachmentName = "Typical attachment name";
                using (var store = GetDocumentStore())
                {
                    using (var session = store.OpenSession())
                    {
                        var user = new User { Name = "su" };
                        session.Store(user, id);
                        session.Advanced.Attachments.Store(id, $"{attachmentName}", stream, "application/zip");
                        session.SaveChanges();
                    }

                    stream.Position = 0;
                    using (var session = store.OpenSession())
                    {
                        var user = session.Load<User>(id);
                        var attachmentsNames = session.Advanced.Attachments.GetNames(user);
                        Dictionary<string, AttachmentResult> attachments = session.Advanced.Attachments.Get(user, attachmentsNames.Select(x => x.Name));

                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];
                        Assert.Equal(stream.Read(buffer1, 0, size), attachments[$"{attachmentName}"].Stream.Read(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));
                    }
                }
            }
        }

        [Fact]
        public async Task CanGetOneAttachmentAsync()
        {
            int size = 1024;
            using (var stream = new MemoryStream(Enumerable.Repeat((byte)0x20, size).ToArray()))
            {
                const string id = "users/1";
                const string attachmentName = "Typical attachment name";
                using (var store = GetDocumentStore())
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = new User { Name = "su" };
                        await session.StoreAsync(user, id);
                        session.Advanced.Attachments.Store(id, $"{attachmentName}", stream, "application/zip");
                        await session.SaveChangesAsync();
                    }

                    stream.Position = 0;
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>(id);
                        var attachmentsNames = session.Advanced.Attachments.GetNames(user);
                        Dictionary<string, AttachmentResult> attachments = await session.Advanced.Attachments.GetAsync(user, attachmentsNames.Select(x => x.Name));

                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];
                        Assert.Equal(await stream.ReadAsync(buffer1, 0, size), await attachments[$"{attachmentName}"].Stream.ReadAsync(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));
                    }
                }
            }
        }

        [Fact]
        public void CanConsumeStream()
        {
            var size = 32 * 1024;
            using (var stream = new MemoryStream(Enumerable.Repeat((byte)0x20, size).ToArray()))
            {
                const string id = "users/1";
                const string attachmentName = "Typical attachment name";
                using (var store = GetDocumentStore())
                {
                    using (var session = store.OpenSession())
                    {
                        var user = new User { Name = "su" };
                        session.Store(user, id);
                        session.Advanced.Attachments.Store(id, $"{attachmentName}", stream, "application/zip");
                        session.SaveChanges();
                    }

                    stream.Position = 0;
                    using (var session = store.OpenSession())
                    {
                        var user = session.Load<User>(id);
                        var attachmentsNames = session.Advanced.Attachments.GetNames(user);
                        Dictionary<string, AttachmentResult> attachments = session.Advanced.Attachments.Get(user, attachmentsNames.Select(x => x.Name));

                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];
                        Assert.Equal(stream.Read(buffer1, 0, size), attachments[$"{attachmentName}"].Stream.Read(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));

                        // consume stream 1
                        buffer1 = new byte[size];
                        buffer2 = new byte[size];
                        Assert.Equal(stream.Read(buffer1, 0, size), attachments[$"{attachmentName}"].Stream.Read(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));

                        // consume stream 2
                        Assert.True(CompareStreams(stream, attachments[$"{attachmentName}"].Stream));
                    }
                }
            }
        }

        [Theory]
        [InlineData(10, 3)]
        [InlineData(10, 1024)]
        [InlineData(10, 32768)]
        [InlineData(100, 3)]
        public void CanGetListOfAttachmentsAndReadParallel(int count, int size)
        {
            var attachmentDictionary = new Dictionary<string, MemoryStream>();
            byte[] bArr = Enumerable.Repeat((byte)0x20, size).ToArray();

            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);
                    Random r = new Random();
                    for (var i = 0; i < count; i++)
                    {
                        var stream = new MemoryStream(bArr);
                        session.Advanced.Attachments.Store(id, $"{attachmentName}_{i}", stream, "application/zip");
                        attachmentDictionary[$"{attachmentName}_{i}"] = stream;
                    }

                    session.SaveChanges();
                }

                foreach (var stream in attachmentDictionary.Values)
                    stream.Position = 0;

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    var attachmentsNames = session.Advanced.Attachments.GetNames(user);
                    Dictionary<string, AttachmentResult> attachments = session.Advanced.Attachments.Get(user, attachmentsNames.Select(x => x.Name));


                    Parallel.For(0, count, RavenTestHelper.DefaultParallelOptions, i =>
                    {
                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];
                        Assert.Equal(attachmentDictionary[$"{attachmentName}_{i}"].Read(buffer1, 0, size), attachments[$"{attachmentName}_{i}"].Stream.Read(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));
                    });
                }
            }

            foreach (var stream in attachmentDictionary.Values)
                stream.Dispose();
        }

        [Theory]
        [InlineData(10, 3)]
        [InlineData(10, 1024)]
        [InlineData(1, 32768)]
        [InlineData(10, 32768)]
        [InlineData(100, 3)]
        public void CanGetListOfAttachmentsAndReadOrdered(int count, int size)
        {
            var attachmentDictionary = new Dictionary<string, MemoryStream>();
            byte[] bArr = Enumerable.Repeat((byte)0x20, size).ToArray();

            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);
                    Random r = new Random();
                    for (var i = 0; i < count; i++)
                    {
                        var stream = new MemoryStream(bArr);
                        session.Advanced.Attachments.Store(id, $"{attachmentName}_{i}", stream, "application/zip");
                        attachmentDictionary[$"{attachmentName}_{i}"] = stream;
                    }

                    session.SaveChanges();
                }

                foreach (var stream in attachmentDictionary.Values)
                    stream.Position = 0;

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    var attachmentsNames = session.Advanced.Attachments.GetNames(user);
                    Dictionary<string, AttachmentResult> attachments = session.Advanced.Attachments.Get(user, attachmentsNames.Select(x => x.Name));

                    for (int i = 0; i < count; i++)
                    {
                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];
                        Assert.Equal(attachmentDictionary[$"{attachmentName}_{i}"].Read(buffer1, 0, size), attachments[$"{attachmentName}_{i}"].Stream.Read(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));
                    }
                }
            }


            foreach (var stream in attachmentDictionary.Values)
                stream.Dispose();
        }

        [Fact]
        public async Task CanGetListOfAttachmentsAndReadOrderedAsync()
        {
            int count = 10;
            int size = 32768;
            var attachmentDictionary = new Dictionary<string, MemoryStream>();
            byte[] bArr = Enumerable.Repeat((byte)0x20, size).ToArray();

            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var user = new User { Name = "su" };
                    await session.StoreAsync(user, id);
                    Random r = new Random();
                    for (var i = 0; i < count; i++)
                    {
                        var stream = new MemoryStream(bArr);
                        session.Advanced.Attachments.Store(id, $"{attachmentName}_{i}", stream, "application/zip");
                        attachmentDictionary[$"{attachmentName}_{i}"] = stream;
                    }

                    await session.SaveChangesAsync();
                }

                foreach (var stream in attachmentDictionary.Values)
                    stream.Position = 0;

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    var attachmentsNames = session.Advanced.Attachments.GetNames(user);
                    Dictionary<string, AttachmentResult> attachments = await session.Advanced.Attachments.GetAsync(user, attachmentsNames.Select(x => x.Name));

                    for (int i = 0; i < count; i++)
                    {
                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];
                        Assert.Equal(await attachmentDictionary[$"{attachmentName}_{i}"].ReadAsync(buffer1, 0, size), await attachments[$"{attachmentName}_{i}"].Stream.ReadAsync(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));
                    }
                }
            }


            foreach (var stream in attachmentDictionary.Values)
                stream.Dispose();
        }

        [Theory]
        [InlineData(10, 3)]
        [InlineData(10, 1024)]
        [InlineData(10, 32768)]
        [InlineData(100, 3)]
        public void CanGetListOfAttachmentsAndReadOrderedPartially(int count, int size)
        {
            var attachmentDictionary = new Dictionary<string, MemoryStream>();
            byte[] bArr = Enumerable.Repeat((byte)0x20, size).ToArray();

            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);
                    Random r = new Random();
                    for (var i = 0; i < count; i++)
                    {
                        var stream = new MemoryStream(bArr);
                        session.Advanced.Attachments.Store(id, $"{attachmentName}_{i}", stream, "application/zip");
                        attachmentDictionary[$"{attachmentName}_{i}"] = stream;
                    }

                    session.SaveChanges();
                }

                foreach (var stream in attachmentDictionary.Values)
                    stream.Position = 0;

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    var attachmentsNames = session.Advanced.Attachments.GetNames(user);
                    Dictionary<string, AttachmentResult> attachments = session.Advanced.Attachments.Get(user, attachmentsNames.Select(x => x.Name));

                    for (int i = 0; i < count; i++)
                    {
                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];

                        const int c = 5;
                        var part = size / c;
                        var sum = 0;
                        for (int j = 0; j < c; j++)
                        {
                            var k = j == c - 1 ? size - sum : part;
                            Assert.Equal(attachmentDictionary[$"{attachmentName}_{i}"].Read(buffer1, j * part, k), attachments[$"{attachmentName}_{i}"].Stream.Read(buffer2, j * part, k));
                            sum = (j + 1) * part;
                        }
                        Assert.Equal(buffer1.Length, size);
                        Assert.True(buffer1.SequenceEqual(buffer2));
                    }
                }
            }


            foreach (var stream in attachmentDictionary.Values)
                stream.Dispose();
        }

        [Theory]
        [InlineData(10, 3)]
        [InlineData(10, 1024)]
        [InlineData(10, 32768)]
        [InlineData(100, 3)]
        public void CanGetListOfAttachmentsAndReadUnordered(int count, int size)
        {
            var attachmentDictionary = new Dictionary<string, MemoryStream>();
            byte[] bArr = Enumerable.Repeat((byte)0x20, size).ToArray();

            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);
                    Random r = new Random();
                    for (var i = 0; i < count; i++)
                    {
                        var stream = new MemoryStream(bArr);
                        session.Advanced.Attachments.Store(id, $"{attachmentName}_{i}", stream, "application/zip");
                        attachmentDictionary[$"{attachmentName}_{i}"] = stream;
                    }

                    session.SaveChanges();
                }

                foreach (var stream in attachmentDictionary.Values)
                    stream.Position = 0;

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    var attachmentsNames = session.Advanced.Attachments.GetNames(user);
                    Dictionary<string, AttachmentResult> attachments = session.Advanced.Attachments.Get(user, attachmentsNames.Select(x => x.Name));

                    for (int i = 0; i < count; i += 2)
                    {
                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];
                        Assert.Equal(attachmentDictionary[$"{attachmentName}_{i}"].Read(buffer1, 0, size), attachments[$"{attachmentName}_{i}"].Stream.Read(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));
                    }

                    for (int i = 1; i < count; i += 2)
                    {
                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];
                        Assert.Equal(attachmentDictionary[$"{attachmentName}_{i}"].Read(buffer1, 0, size), attachments[$"{attachmentName}_{i}"].Stream.Read(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));
                    }
                }
            }


            foreach (var stream in attachmentDictionary.Values)
                stream.Dispose();
        }

        [Theory]
        [InlineData(10, 3)]
        [InlineData(10, 1024)]
        [InlineData(10, 32768)]
        [InlineData(100, 3)]
        public void CanGetListOfAttachmentsAndReadUnorderedPartially(int count, int size)
        {
            var attachmentDictionary = new Dictionary<string, MemoryStream>();
            byte[] bArr = Enumerable.Repeat((byte)0x20, size).ToArray();

            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);
                    Random r = new Random();
                    for (var i = 0; i < count; i++)
                    {
                        var stream = new MemoryStream(bArr);
                        session.Advanced.Attachments.Store(id, $"{attachmentName}_{i}", stream, "application/zip");
                        attachmentDictionary[$"{attachmentName}_{i}"] = stream;
                    }

                    session.SaveChanges();
                }

                foreach (var stream in attachmentDictionary.Values)
                    stream.Position = 0;

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    var attachmentsNames = session.Advanced.Attachments.GetNames(user);
                    Dictionary<string, AttachmentResult> attachments = session.Advanced.Attachments.Get(user, attachmentsNames.Select(x => x.Name));

                    var buffer1 = new byte[size];
                    var buffer2 = new byte[size];
                    attachments[$"{attachmentName}_0"].Stream.Read(buffer1, 0, 1);
                    Assert.True(CompareStreams(attachmentDictionary[$"{attachmentName}_3"], attachments[$"{attachmentName}_3"].Stream));
                    attachments[$"{attachmentName}_0"].Stream.Read(buffer1, 1, 1);
                    Assert.True(CompareStreams(attachmentDictionary[$"{attachmentName}_2"], attachments[$"{attachmentName}_2"].Stream));
                    attachments[$"{attachmentName}_0"].Stream.Read(buffer1, 2, size - 2);
                    attachmentDictionary[$"{attachmentName}_0"].Read(buffer2, 0, size);
                    Assert.True(buffer1.SequenceEqual(buffer2));
                    Assert.True(CompareStreams(attachmentDictionary[$"{attachmentName}_1"], attachments[$"{attachmentName}_1"].Stream));

                    for (int i = 4; i < count; i++)
                    {
                        Assert.True(CompareStreams(attachmentDictionary[$"{attachmentName}_{i}"], attachments[$"{attachmentName}_{i}"].Stream));
                    }

                }
            }


            foreach (var stream in attachmentDictionary.Values)
                stream.Dispose();
        }

        [Theory]
        [InlineData(10, 3)]
        [InlineData(10, 1024)]
        [InlineData(10, 32768)]
        [InlineData(100, 3)]
        [InlineData(1000, 128)]
        public void CanGetListOfAttachmentsAndReadRandom(int count, int size)
        {
            var attachmentDictionary = new Dictionary<string, MemoryStream>();
            byte[] bArr = Enumerable.Repeat((byte)0x20, size).ToArray();

            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);
                    Random r = new Random();
                    for (var i = 0; i < count; i++)
                    {
                        var stream = new MemoryStream(bArr);
                        session.Advanced.Attachments.Store(id, $"{attachmentName}_{i}", stream, "application/zip");
                        attachmentDictionary[$"{attachmentName}_{i}"] = stream;
                    }

                    session.SaveChanges();
                }

                foreach (var stream in attachmentDictionary.Values)
                    stream.Position = 0;

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    var attachmentsNames = session.Advanced.Attachments.GetNames(user);
                    Dictionary<string, AttachmentResult> attachments = session.Advanced.Attachments.Get(user, attachmentsNames.Select(x => x.Name));
                    var randomReads = new List<int>();
                    Random rnd = new Random();

                    byte[] buffer1;
                    byte[] buffer2;

                    var partial = false;
                    var partialIndex = 0;

                    for (int i = 0; i < count / 10; i++)
                    {
                        buffer1 = new byte[size];
                        buffer2 = new byte[size];
                        var index = rnd.Next(0, count - 1);
                        if (randomReads.Contains(index) == false)
                        {
                            randomReads.Add(index);
                            var magicNum = partial ? rnd.Next(0, 1) : rnd.Next(0, 2);
                            if (magicNum == 0)
                            {
                                Assert.True(CompareStreams(attachmentDictionary[$"{attachmentName}_{index}"], attachments[$"{attachmentName}_{index}"].Stream));
                            }
                            else if (magicNum == 1)
                            {
                                Assert.Equal(attachmentDictionary[$"{attachmentName}_{index}"].Read(buffer1, 0, size), attachments[$"{attachmentName}_{index}"].Stream.Read(buffer2, 0, size));
                                Assert.True(buffer1.SequenceEqual(buffer2));
                            }
                            else
                            {
                                Assert.Equal(attachmentDictionary[$"{attachmentName}_{index}"].Read(buffer1, 0, size / 3), attachments[$"{attachmentName}_{index}"].Stream.Read(buffer2, 0, size / 3));
                                Assert.True(buffer1.SequenceEqual(buffer2));
                                partial = true;
                                partialIndex = index;
                            }
                        }
                    }

                    for (int i = 0; i < count; i++)
                    {
                        if (randomReads.Contains(i))
                            continue;

                        buffer1 = new byte[size];
                        buffer2 = new byte[size];

                        Assert.Equal(attachmentDictionary[$"{attachmentName}_{i}"].Read(buffer1, 0, size), attachments[$"{attachmentName}_{i}"].Stream.Read(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));
                    }

                    if (partial)
                    {
                        buffer1 = new byte[size];
                        buffer2 = new byte[size];
                        Assert.Equal(attachmentDictionary[$"{attachmentName}_{partialIndex}"].Read(buffer1, size / 3, size - (size / 3)), attachments[$"{attachmentName}_{partialIndex}"].Stream.Read(buffer2, size, size - (size / 3)));
                        Assert.True(buffer1.SequenceEqual(buffer2));
                    }
                }
            }


            foreach (var stream in attachmentDictionary.Values)
                stream.Dispose();
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(15)]
        public void CanGetListOfDifferentAttachmentsAndRead(int count)
        {
            var attachmentDictionary = new Dictionary<string, MemoryStream>();

            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            var factorials = new List<int>();
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);
                    Random r = new Random();
                    for (var i = 0; i < count; i++)
                    {
                        var factorial = Factorial(i);
                        factorials.Add(factorial);
                        byte[] bArr = Enumerable.Repeat((byte)0x20, factorial).ToArray();
                        var stream = new MemoryStream(bArr);
                        session.Advanced.Attachments.Store(id, $"{attachmentName}_{i}", stream, "application/zip");
                        attachmentDictionary[$"{attachmentName}_{i}"] = stream;
                    }

                    session.SaveChanges();
                }

                foreach (var stream in attachmentDictionary.Values)
                    stream.Position = 0;

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    var attachmentsNames = session.Advanced.Attachments.GetNames(user);
                    Dictionary<string, AttachmentResult> attachments = session.Advanced.Attachments.Get(user, attachmentsNames.Select(x => x.Name));

                    for (int i = 0; i < count; i++)
                    {
                        var size = factorials[i];
                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];
                        Assert.Equal(attachmentDictionary[$"{attachmentName}_{i}"].Read(buffer1, 0, size), attachments[$"{attachmentName}_{i}"].Stream.Read(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));
                    }
                }
            }


            foreach (var stream in attachmentDictionary.Values)
                stream.Dispose();
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        // [InlineData(15)] todo: move to stress test
        public void CanGetListOfDifferentAttachmentsAndReadRandom(int count)
        {
            var attachmentDictionary = new Dictionary<string, MemoryStream>();

            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            var factorials = new List<int>();
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);
                    Random r = new Random();
                    for (var i = 0; i < count; i++)
                    {
                        var factorial = Factorial(i);
                        factorials.Add(factorial);
                        byte[] bArr = Enumerable.Repeat((byte)0x20, factorial).ToArray();
                        var stream = new MemoryStream(bArr);
                        session.Advanced.Attachments.Store(id, $"{attachmentName}_{i}", stream, "application/zip");
                        attachmentDictionary[$"{attachmentName}_{i}"] = stream;
                    }

                    session.SaveChanges();
                }

                foreach (var stream in attachmentDictionary.Values)
                    stream.Position = 0;

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    var attachmentsNames = session.Advanced.Attachments.GetNames(user);
                    Dictionary<string, AttachmentResult> attachments = session.Advanced.Attachments.Get(user, attachmentsNames.Select(x => x.Name));
                    var randomReads = new List<int>();
                    Random rnd = new Random();

                    byte[] buffer1;
                    byte[] buffer2;

                    var partial = false;
                    var partialIndex = 0;

                    for (int i = 0; i < count / 10; i++)
                    {
                        var index = rnd.Next(0, count - 1);

                        buffer1 = new byte[factorials[index]];
                        buffer2 = new byte[factorials[index]];

                        if (randomReads.Contains(index) == false)
                        {
                            randomReads.Add(index);
                            var magicNum = partial ? rnd.Next(0, 1) : rnd.Next(0, 2);
                            if (magicNum == 0)
                            {
                                Assert.True(CompareStreams(attachmentDictionary[$"{attachmentName}_{index}"], attachments[$"{attachmentName}_{index}"].Stream));
                            }
                            else if (magicNum == 1)
                            {
                                Assert.Equal(attachmentDictionary[$"{attachmentName}_{index}"].Read(buffer1, 0, factorials[index]), attachments[$"{attachmentName}_{index}"].Stream.Read(buffer2, 0, factorials[index]));
                                Assert.True(buffer1.SequenceEqual(buffer2));
                            }
                            else
                            {
                                Assert.Equal(attachmentDictionary[$"{attachmentName}_{index}"].Read(buffer1, 0, factorials[index] / 3), attachments[$"{attachmentName}_{index}"].Stream.Read(buffer2, 0, factorials[index] / 3));
                                Assert.True(buffer1.SequenceEqual(buffer2));
                                partial = true;
                                partialIndex = index;
                            }
                        }
                    }

                    for (int i = 0; i < count; i++)
                    {
                        if (randomReads.Contains(i))
                            continue;

                        buffer1 = new byte[factorials[i]];
                        buffer2 = new byte[factorials[i]];

                        Assert.Equal(attachmentDictionary[$"{attachmentName}_{i}"].Read(buffer1, 0, factorials[i]), attachments[$"{attachmentName}_{i}"].Stream.Read(buffer2, 0, factorials[i]));
                        Assert.True(buffer1.SequenceEqual(buffer2));
                    }

                    if (partial)
                    {
                        buffer1 = new byte[factorials[partialIndex]];
                        buffer2 = new byte[factorials[partialIndex]];
                        Assert.Equal(attachmentDictionary[$"{attachmentName}_{partialIndex}"].Read(buffer1, factorials[partialIndex] / 3, factorials[partialIndex] - (factorials[partialIndex] / 3)), attachments[$"{attachmentName}_{partialIndex}"].Stream.Read(buffer2, factorials[partialIndex], factorials[partialIndex] - (factorials[partialIndex] / 3)));
                        Assert.True(buffer1.SequenceEqual(buffer2));
                    }
                }
            }


            foreach (var stream in attachmentDictionary.Values)
                stream.Dispose();
        }

        [Fact]
        public void CanSendEmptyList()
        {
            const string id = "users/1";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    var attachments = session.Advanced.Attachments.Get(user, new string[] { });

                    Assert.Empty(attachments);
                }
            }
        }

        private static bool CompareStreams(Stream a, Stream b)
        {
            if (a == null && b == null)
                return true;

            if (a == null || b == null)
                return false;

            if (a.Length < b.Length)
                return false;

            if (a.Length > b.Length)
                return false;

            for (int i = 0; i < a.Length; i++)
            {
                int aByte = a.ReadByte();
                int bByte = b.ReadByte();

                if (aByte.CompareTo(bByte) != 0)
                    return false;
            }

            return true;
        }

        private static int Factorial(int n)
        {
            if (n < 0)
                throw new ArgumentException("cant get Factorial of negative number");

            if (n == 0)
                return 1;

            int sum = n;
            int result = n;
            for (int i = n - 2; i > 1; i -= 2)
            {
                sum = (sum + i);
                result *= sum;
            }

            if (n % 2 != 0)
                result *= n / 2 + 1;

            return result;
        }
    }
}

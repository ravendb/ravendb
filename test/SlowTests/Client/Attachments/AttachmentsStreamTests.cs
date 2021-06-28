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
    public class AttachmentsStreamTests : RavenTestBase
    {
        public AttachmentsStreamTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(1024)]
        [InlineData(32 * 1024)]
        [InlineData(256 * 1024)]
        [InlineData(1024 * 1024)]
        [InlineData(128 * 1024 * 1024)]
        public void CanGetOneAttachment(int size)
        {
            var rnd = new Random();
            var b = new byte[size];
            rnd.NextBytes(b);

            using (var stream = new MemoryStream(b))
            {
                const string id = "users/1";
                const string attachmentName = "Typical attachment name";
                using (var store = GetDocumentStore())
                {
                    using (var session = store.OpenSession())
                    {
                        var user = new User { Name = "su" };
                        session.Store(user, id);
                        session.SaveChanges();
                        session.SaveChanges();
                    }
                    store.Operations.Send(new PutAttachmentOperation(id, $"{attachmentName}", stream, "application/zip"));

                    stream.Position = 0;
                    using (var session = store.OpenSession())
                    {
                        var user = session.Load<User>(id);
                        var attachmentsNames = session.Advanced.Attachments.GetNames(user).Select(x => new AttachmentRequest(id, x.Name));
                        var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);

                        while (attachmentsEnumerator.MoveNext())
                        {
                            Assert.NotNull(attachmentsEnumerator.Current != null);
                            Assert.True(CompareStreams(attachmentsEnumerator.Current.Stream, stream));
                            attachmentsEnumerator.Current?.Stream?.Dispose();
                        }
                    }
                }
            }
        }

        [Theory]
        [InlineData(1024)]
        [InlineData(32 * 1024)]
        [InlineData(256 * 1024)]
        [InlineData(1024 * 1024)]
        [InlineData(128 * 1024 * 1024)]
        public async Task CanGetOneAttachmentAsync(int size)
        {
            var rnd = new Random();
            var b = new byte[size];
            rnd.NextBytes(b);
            using (var stream = new MemoryStream(b))
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
                        var attachmentsNames = session.Advanced.Attachments.GetNames(user).Select(x => new AttachmentRequest(id, x.Name));
                        var attachmentsEnumerator = await session.Advanced.Attachments.GetAsync(attachmentsNames);
                        while (attachmentsEnumerator.MoveNext())
                        {
                            var current = attachmentsEnumerator.Current;
                            Assert.NotNull(current);
                            Assert.True(CompareStreams(current.Stream, stream));
                            current.Stream?.Dispose();
                        }
                    }
                }
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(100)]
        public void CanConsumeStream(int count)
        {
            var size = 32 * 1024;

            var attachmentDictionary = new Dictionary<string, MemoryStream>();
            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);
                    for (var i = 0; i < count; i++)
                    {
                        var rnd = new Random();
                        var b = new byte[size];
                        rnd.NextBytes(b);
                        var stream = new MemoryStream(b);
                        session.Advanced.Attachments.Store(id, $"{attachmentName}_{i}", stream, "application/zip");
                        attachmentDictionary[$"{attachmentName}_{i}"] = stream;
                    }
                    session.SaveChanges();
                }

                foreach (var s in attachmentDictionary.Values)
                    s.Position = 0;

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(id);
                    var attachmentsNames = session.Advanced.Attachments.GetNames(user).Select(x => new AttachmentRequest(id, x.Name));
                    var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);
                    while (attachmentsEnumerator.MoveNext())
                    {
                        using (var memoryStream = new MemoryStream())
                        {
                            var attachmentResult = attachmentsEnumerator.Current;

                            Assert.NotNull(attachmentResult);
                            attachmentResult.Stream.CopyTo(memoryStream);
                            memoryStream.Position = 0;
                            attachmentResult.Stream.CopyTo(memoryStream);

                            Assert.Equal(0, memoryStream.Position);

                            var buffer1 = new byte[size];
                            var buffer2 = new byte[size];

                            Assert.Equal(attachmentDictionary[$"{attachmentResult.Details.Name}"].Read(buffer1, 0, size), memoryStream.Read(buffer2, 0, size));
                            Assert.True(buffer1.SequenceEqual(buffer2));

                            attachmentResult.Stream?.Dispose();
                        }
                    }
                }
            }
        }

        [Theory]
        [InlineData(10, 3)]
        [InlineData(10, 1024)]
        [InlineData(1, 32768)]
        [InlineData(10, 32768)]
        [InlineData(100, 3)]
        public void CanGetListOfAttachmentsAndSkip(int count, int size)
        {
            var attachmentDictionary = new Dictionary<string, MemoryStream>();
            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);

                    for (var i = 0; i < count; i++)
                    {
                        var rnd = new Random();
                        var bArr = new byte[size];
                        rnd.NextBytes(bArr);
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

                    var attachmentsNames = session.Advanced.Attachments.GetNames(user).Select(x => new AttachmentRequest(id, x.Name));
                    var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);
                    Random rndRnd = new Random();

                    var skippedIndexes = new List<string>();
                    while (attachmentsEnumerator.MoveNext())
                    {
                        if (rndRnd.Next(0, 2) == 0)
                        {
                            if (attachmentsEnumerator.Current != null)
                            {
                                skippedIndexes.Add(attachmentsEnumerator.Current.Details.Name);
                            }
                            continue;
                        }

                        Assert.NotNull(attachmentsEnumerator.Current);
                        Assert.True(CompareStreams(attachmentsEnumerator.Current.Stream, attachmentDictionary[$"{attachmentsEnumerator.Current.Details.Name}"], compareByteArray: true), $"Skipped Attachments: {string.Join(" ", skippedIndexes)}");
                        attachmentsEnumerator.Current.Stream?.Dispose();
                    }
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
            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);
                    for (var i = 0; i < count; i++)
                    {
                        var rnd = new Random();
                        var bArr = new byte[size];
                        rnd.NextBytes(bArr);
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

                    var attachmentsNames = session.Advanced.Attachments.GetNames(user).Select(x => new AttachmentRequest(id, x.Name));
                    var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);
                    while (attachmentsEnumerator.MoveNext())
                    {
                        Assert.NotNull(attachmentsEnumerator.Current);
                        Assert.True(CompareStreams(attachmentsEnumerator.Current.Stream, attachmentDictionary[$"{attachmentsEnumerator.Current.Details.Name}"], compareByteArray: true));
                        attachmentsEnumerator.Current.Stream?.Dispose();
                    }
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
        public void ShouldThrowOnDisposedStream(int count, int size)
        {
            var attachmentDictionary = new Dictionary<string, MemoryStream>();
            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);
                    for (var i = 0; i < count; i++)
                    {
                        var rnd = new Random();
                        var bArr = new byte[size];
                        rnd.NextBytes(bArr);
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

                    var attachmentsNames = session.Advanced.Attachments.GetNames(user).Select(x => new AttachmentRequest(id, x.Name));
                    var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);
                    while (attachmentsEnumerator.MoveNext())
                    {
                    }

                    Assert.Throws<ObjectDisposedException>(() => attachmentsEnumerator.Current.Stream.Read(new byte[1], 0, 1));
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
        public void ShouldThrowOnAccessingDisposedAttachment(int count, int size)
        {
            var attachmentDictionary = new Dictionary<string, MemoryStream>();
            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);
                    for (var i = 0; i < count; i++)
                    {
                        var rnd = new Random();
                        var bArr = new byte[size];
                        rnd.NextBytes(bArr);
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

                    var attachmentsNames = session.Advanced.Attachments.GetNames(user).Select(x => new AttachmentRequest(id, x.Name));
                    var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);

                    var skip = true;
                    while (attachmentsEnumerator.MoveNext())
                    {
                        if (skip)
                        {
                            using (var stream = attachmentsEnumerator.Current.Stream)
                            {
                                if (attachmentsEnumerator.MoveNext() == false)
                                {
                                    Assert.Throws<ObjectDisposedException>(() => stream.Read(new byte[1], 0, 1));
                                    continue;
                                }

                                Assert.Throws<ObjectDisposedException>(() => stream.Read(new byte[1], 0, 1));
                            }

                            skip = false;
                        }

                        var rnd = new Random();
                        var n = rnd.Next(0, 2);
                        if (n == 0)
                        {
                            attachmentsEnumerator.Current.Stream.Dispose();
                            Assert.Throws<ObjectDisposedException>(() => attachmentsEnumerator.Current.Stream.Read(new byte[1], 0, 1));
                        }
                        else if (n == 1)
                        {
                            using (var memoryStream = new MemoryStream())
                            {
                                attachmentsEnumerator.Current.Stream.CopyTo(memoryStream);
                                memoryStream.Position = 0;

                                var buffer1 = new byte[size];
                                var buffer2 = new byte[size];

                                Assert.Equal(attachmentDictionary[$"{attachmentsEnumerator.Current.Details.Name}"].Read(buffer1, 0, size), memoryStream.Read(buffer2, 0, size));
                                Assert.True(buffer1.SequenceEqual(buffer2));
                                attachmentsEnumerator.Current.Stream?.Dispose();
                            }
                        }
                    }
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
        public void CanGetListOfAttachmentsAndReadPartially(int count, int size)
        {
            var attachmentDictionary = new Dictionary<string, MemoryStream>();
            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);
                    for (var i = 0; i < count; i++)
                    {
                        var rnd = new Random();
                        var bArr = new byte[size];
                        rnd.NextBytes(bArr);
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

                    var attachmentsNames = session.Advanced.Attachments.GetNames(user).Select(x => new AttachmentRequest(id, x.Name));
                    var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);
                    var rndRnd = new Random();
                    while (attachmentsEnumerator.MoveNext())
                    {
                        using (var memoryStream = new MemoryStream())
                        {
                            var attachmentResult = attachmentsEnumerator.Current;

                            Assert.NotNull(attachmentsEnumerator.Current);

                            if (rndRnd.Next(0, 2) == 0)
                            {
                                var s = size / 3;
                                var buffer1 = new byte[s];
                                var buffer2 = new byte[s];
                                attachmentDictionary[$"{attachmentResult.Details.Name}"].Read(buffer1, 0, s);

                                var toRead = s;
                                var r = 0;
                                while (toRead > 0)
                                {
                                    r = attachmentResult.Stream.Read(buffer2, 0 + r, toRead);
                                    toRead -= r;
                                }

                                Assert.True(buffer1.SequenceEqual(buffer2));
                            }
                            else
                            {
                                attachmentResult.Stream.CopyTo(memoryStream);
                                memoryStream.Position = 0;

                                var buffer1 = new byte[size];
                                var buffer2 = new byte[size];

                                Assert.Equal(attachmentDictionary[$"{attachmentResult.Details.Name}"].Read(buffer1, 0, size), memoryStream.Read(buffer2, 0, size));
                                Assert.True(buffer1.SequenceEqual(buffer2));
                            }

                            attachmentResult.Stream?.Dispose();
                        }

                    }
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
        public async Task CanGetListOfAttachmentsAndReadOrderedAsync(int count, int size)
        {
            var attachmentDictionary = new Dictionary<string, MemoryStream>();

            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var user = new User { Name = "su" };
                    await session.StoreAsync(user, id);
                    for (var i = 0; i < count; i++)
                    {
                        var rnd = new Random();
                        var b = new byte[size];
                        rnd.NextBytes(b);
                        var stream = new MemoryStream(b);
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
                    var attachmentsNames = session.Advanced.Attachments.GetNames(user).Select(x => new AttachmentRequest(id, x.Name));
                    var attachmentsEnumerator = await session.Advanced.Attachments.GetAsync(attachmentsNames);
                    while (attachmentsEnumerator.MoveNext())
                    {
                        var current = attachmentsEnumerator.Current;
                        Assert.NotNull(current);
                        Assert.True(await CompareStreamsAsync(current.Stream, attachmentDictionary[$"{current.Details.Name}"]));
                        current.Stream?.Dispose();
                    }
                }
            }

            foreach (var stream in attachmentDictionary.Values)
                stream.Dispose();
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
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
                    for (var i = 0; i < count; i++)
                    {
                        var factorial = Factorial(i);
                        factorials.Add(factorial);
                        var rnd = new Random();
                        var b = new byte[factorial];
                        rnd.NextBytes(b);
                        var stream = new MemoryStream(b);
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
                    var attachmentsNames = session.Advanced.Attachments.GetNames(user).Select(x => new AttachmentRequest(id, x.Name));
                    var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);
                    int i = 0;
                    while (attachmentsEnumerator.MoveNext())
                    {
                        var size = factorials[i];

                        using (var memoryStream = new MemoryStream())
                        {
                            var attachmentResult = attachmentsEnumerator.Current;

                            Assert.NotNull(attachmentResult);
                            attachmentResult.Stream.CopyTo(memoryStream);
                            memoryStream.Position = 0;

                            var buffer1 = new byte[size];
                            var buffer2 = new byte[size];

                            Assert.Equal(attachmentDictionary[$"{attachmentResult.Details.Name}"].Read(buffer1, 0, size), memoryStream.Read(buffer2, 0, size));
                            Assert.True(buffer1.SequenceEqual(buffer2));
                            i++;

                            attachmentResult.Stream?.Dispose();
                        }
                    }
                }
            }

            foreach (var stream in attachmentDictionary.Values)
                stream.Dispose();
        }

        [Fact]
        public void CanSendNonExistingListOfAttachments()
        {
            var rnd = new Random();
            var b = new byte[rnd.Next(1, 64 * 1024)];
            rnd.NextBytes(b);

            using (var stream = new MemoryStream(b))
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

                    using (var session = store.OpenSession())
                    {
                        var attachmentsEnumerator = session.Advanced.Attachments.Get(new AttachmentRequest[] { });

                        Assert.False(attachmentsEnumerator.MoveNext());
                    }

                    using (var session = store.OpenSession())
                    {
                        var attachmentsEnumerator = session.Advanced.Attachments.Get(new[] { new AttachmentRequest("users/1", "1") });

                        Assert.False(attachmentsEnumerator.MoveNext());
                    }

                    using (var session = store.OpenSession())
                    {
                        var attachmentsEnumerator = session.Advanced.Attachments.Get(new[] { new AttachmentRequest("users/2", "1") });

                        Assert.False(attachmentsEnumerator.MoveNext());
                    }

                    using (var session = store.OpenSession())
                    {
                        var attachmentsEnumerator = session.Advanced.Attachments.Get(new[]
                        {
                            new AttachmentRequest("users/1", "1"),
                            new AttachmentRequest("users/1", $"{attachmentName}"),
                            new AttachmentRequest("users/2", "1")
                        });
                        attachmentsEnumerator.MoveNext();
                        Assert.NotNull(attachmentsEnumerator.Current);
                        Assert.Equal(attachmentsEnumerator.Current.Details.Name, attachmentName);
                        attachmentsEnumerator.Current.Stream?.Dispose();
                        Assert.False(attachmentsEnumerator.MoveNext());
                    }
                }
            }
        }

        internal static bool CompareStreams(Stream a, Stream b, bool compareByteArray = false)
        {
            if (a == null && b == null)
                return true;

            if (a == null || b == null)
                throw new ArgumentNullException(a == null ? "a" : "b");

            if (a.Length < b.Length)
                return false;

            if (a.Length > b.Length)
                return false;

            int s = (int)Math.Min(32 * 1024, a.Length);

            var buffer1 = new byte[s];
            var buffer2 = new byte[s];

            var toRead = a.Length;
            while (toRead > 0)
            {
                var read1 = 0;
                var read2 = 0;
                int r = (int)Math.Min(s, toRead);

                while (r - read1 > 0)
                {
                    var r1 = a.Read(buffer1, read1, r - read1);
                    read1 += r1;
                }

                while (r - read2 > 0)
                {
                    var r2 = b.Read(buffer2, read2, r - read2);
                    read2 += r2;
                }

                Assert.Equal(read1, read2);

                if (compareByteArray)
                {
                    for (int i = 0; i < read1; i++)
                    {
                        if (buffer1[i] != buffer2[i])
                        {
                            return false;
                        }
                    }
                }
                else
                {
                    if (new ReadOnlySpan<byte>(buffer1).SequenceEqual(new ReadOnlySpan<byte>(buffer2)) == false)
                        return false;
                }

                toRead -= read1;
            }

            return true;
        }

        private static async Task<bool> CompareStreamsAsync(Stream a, Stream b)
        {
            if (a == null && b == null)
                return true;

            if (a == null || b == null)
                throw new ArgumentNullException(a == null ? "a" : "b");

            if (a.Length < b.Length)
                return false;

            if (a.Length > b.Length)
                return false;

            int s = (int)Math.Min(32 * 1024, a.Length);

            var buffer1 = new byte[s];
            var buffer2 = new byte[s];

            var toRead = a.Length;
            while (toRead > 0)
            {
                var read1 = 0;
                var read2 = 0;
                int r = (int)Math.Min(s, toRead);

                while (r - read1 > 0)
                {
                    var r1 = await a.ReadAsync(buffer1, read1, r - read1);
                    read1 += r1;
                }

                while (r - read2 > 0)
                {
                    var r2 = await b.ReadAsync(buffer2, read2, r - read2);
                    read2 += r2;
                }

                Assert.Equal(read1, read2);

                for (int i = 0; i < read1; i++)
                {
                    if (buffer1[i] != buffer2[i])
                    {
                        return false;
                    }
                }

                toRead -= read1;
            }

            return true;
        }

        internal static int Factorial(int n)
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

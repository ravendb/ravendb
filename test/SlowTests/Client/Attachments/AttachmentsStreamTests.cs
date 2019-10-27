using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Exceptions;
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
                        session.Advanced.Attachments.Store(id, $"{attachmentName}", stream, "application/zip");
                        session.SaveChanges();
                    }

                    stream.Position = 0;
                    using (var session = store.OpenSession())
                    {
                        var user = session.Load<User>(id);
                        var attachmentsNames = session.Advanced.Attachments.GetNames(user).Select(x => new AttachmentRequest(id, x.Name));
                        var attachmentsEnumerator = session.Advanced.Attachments.Get(attachmentsNames);
                        var memoryStream = new MemoryStream();

                        while (attachmentsEnumerator.MoveNext())
                        {
                            Assert.NotNull(attachmentsEnumerator.Current != null);
                            attachmentsEnumerator.Current.Stream.CopyTo(memoryStream);
                            memoryStream.Position = 0;
                        }
                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];

                        Assert.Equal(stream.Read(buffer1, 0, size), memoryStream.Read(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));
                    }
                }
            }
        }

        [Fact]
        public async Task CanGetOneAttachmentAsync()
        {
            int size = 1024;
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
                        var memoryStream = new MemoryStream();

                        while (attachmentsEnumerator.MoveNext())
                        {
                            Assert.NotNull(attachmentsEnumerator.Current != null);
                            await attachmentsEnumerator.Current.Stream.CopyToAsync(memoryStream);
                            memoryStream.Position = 0;
                        }
                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];
                        Assert.Equal(await stream.ReadAsync(buffer1, 0, size), await memoryStream.ReadAsync(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));
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
                        var memoryStream = new MemoryStream();
                        var attachmentResult = attachmentsEnumerator.Current;

                        Assert.NotNull(attachmentsEnumerator.Current != null);
                        attachmentResult.Stream.CopyTo(memoryStream);
                        memoryStream.Position = 0;
                        attachmentResult.Stream.CopyTo(memoryStream);

                        Assert.Equal(0, memoryStream.Position);

                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];

                        Assert.Equal(attachmentDictionary[$"{attachmentResult.Details.Name}"].Read(buffer1, 0, size), memoryStream.Read(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));
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
        [InlineData(1000, 1)]
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

                        var memoryStream = new MemoryStream();
                        var attachmentResult = attachmentsEnumerator.Current;

                        Assert.NotNull(attachmentsEnumerator.Current != null);
                        attachmentResult.Stream.CopyTo(memoryStream);
                        memoryStream.Position = 0;

                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];

                        Assert.Equal(attachmentDictionary[$"{attachmentResult.Details.Name}"].Read(buffer1, 0, size), memoryStream.Read(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2), $"Skipped Attachments: {string.Join(" ", skippedIndexes)}");
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
                        var memoryStream = new MemoryStream();
                        var attachmentResult = attachmentsEnumerator.Current;

                        Assert.NotNull(attachmentsEnumerator.Current != null);
                        attachmentResult.Stream.CopyTo(memoryStream);
                        memoryStream.Position = 0;

                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];

                        Assert.Equal(attachmentDictionary[$"{attachmentResult.Details.Name}"].Read(buffer1, 0, size), memoryStream.Read(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));
                    }
                }
            }

            foreach (var stream in attachmentDictionary.Values)
                stream.Dispose();
        }

        [Fact]
        public void ShouldThrowOnDisposedStream()
        {
            int count = 2;
            int size = 32768;
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

                   Assert.Throws<StreamDisposedException>(() => attachmentsEnumerator.Current.Stream.Read(new byte[1], 0, 1));
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
                        var memoryStream = new MemoryStream();
                        var attachmentResult = attachmentsEnumerator.Current;

                        Assert.NotNull(attachmentsEnumerator.Current != null);

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
                        var memoryStream = new MemoryStream();
                        var attachmentResult = attachmentsEnumerator.Current;

                        Assert.NotNull(attachmentResult != null);
                        await attachmentResult.Stream.CopyToAsync(memoryStream);

                        memoryStream.Position = 0;

                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];

                        Assert.Equal(attachmentDictionary[$"{attachmentResult.Details.Name}"].Read(buffer1, 0, size), memoryStream.Read(buffer2, 0, size));
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

                        var memoryStream = new MemoryStream();
                        var attachmentResult = attachmentsEnumerator.Current;

                        Assert.NotNull(attachmentsEnumerator.Current != null);
                        attachmentResult.Stream.CopyTo(memoryStream);
                        memoryStream.Position = 0;

                        var buffer1 = new byte[size];
                        var buffer2 = new byte[size];

                        Assert.Equal(attachmentDictionary[$"{attachmentResult.Details.Name}"].Read(buffer1, 0, size), memoryStream.Read(buffer2, 0, size));
                        Assert.True(buffer1.SequenceEqual(buffer2));
                        i++;
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

                        Assert.False(attachmentsEnumerator.MoveNext());
                    }
                }
            }
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

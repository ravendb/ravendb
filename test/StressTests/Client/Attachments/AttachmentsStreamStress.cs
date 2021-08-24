using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.Attachments;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Client.Attachments
{
    public class AttachmentsStreamStress : RavenTestBase
    {
        public AttachmentsStreamStress(ITestOutputHelper output) : base(output)
        {
        }

        [Theory64Bit]
        [InlineData(3, (long)int.MaxValue + short.MaxValue)]
        public void CanGetListOfHugeAttachmentsAndReadOrdered(int count, long size)
        {
            var attachmentDictionary = new Dictionary<string, BigDummyStream>();
            const string id = "users/1";
            const string attachmentName = "Typical attachment name";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "su" };
                    session.Store(user, id);
                    session.SaveChanges();
                }

                for (var i = 0; i < count; i++)
                {
                    var bigStream = new BigDummyStream(size);
                    store.Operations.Send(new PutAttachmentOperation("users/1", $"{attachmentName}_{i}", bigStream));
                    attachmentDictionary[$"{attachmentName}_{i}"] = bigStream;
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
                        Assert.True(AttachmentsStreamTests.CompareStreams(attachmentsEnumerator.Current.Stream, attachmentDictionary[$"{attachmentsEnumerator.Current.Details.Name}"]));
                        attachmentsEnumerator.Current.Stream?.Dispose();
                    }
                }
            }

            foreach (var stream in attachmentDictionary.Values)
                stream.Dispose();
        }

        [Theory64Bit]
        [InlineData(14)]
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
                        var factorial = AttachmentsStreamTests.Factorial(i);
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
                            attachmentResult.Stream?.Dispose();
                            i++;
                        }
                    }
                }
            }

            foreach (var stream in attachmentDictionary.Values)
                stream.Dispose();
        }
    }
}

using Raven.Abstractions.FileSystem;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem.Notifications;

namespace RavenFS.Tests.ClientApi
{
    public class SerializationTests
    {

        [Fact]
        public void FileHeaderSerialization()
        {
            var serializer = new JsonSerializer();


            var metadata = new RavenJObject { {"Last-Modified", "2014-07-07T12:00:00.0000000"}, { "RavenFS-Size", "128" } };
            var fileHeader = new FileHeader("test1.file", metadata);

            var serializedValue = JsonExtensions.ToJObject(fileHeader);

            var jr = new RavenJTokenReader(serializedValue);
            var deserializedValue = JsonExtensions.CreateDefaultJsonSerializer().Deserialize<FileHeader>(jr);

            Assert.NotNull(deserializedValue);
            Assert.Equal(fileHeader.Name, deserializedValue.Name);
            Assert.Equal(fileHeader.LastModified, deserializedValue.LastModified);
        }

        [Fact]
        public void ConflictNotificationSerialization()
        {
            var serializer = new JsonSerializer();


            var metadata = new RavenJObject { { "Last-Modified", "2014-07-07T12:00:00.0000000" }, { "RavenFS-Size", "128" } };
            var fileHeader = new FileHeader("test1.file", metadata);
            var notification = new ConflictNotification() { FileName = "test1.file", SourceServerUrl = "http://destination", RemoteFileHeader = fileHeader, Status = ConflictStatus.Detected};

            var serializedValue = JsonExtensions.ToJObject(notification);

            var jr = new RavenJTokenReader(serializedValue);
            var deserializedValue = JsonExtensions.CreateDefaultJsonSerializer().Deserialize<ConflictNotification>(jr);

            Assert.NotNull(deserializedValue);
        }
    }
}

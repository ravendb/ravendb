using System;
using System.Collections.Generic;
using System.Data.HashFunction.Blake2;
using System.Dynamic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Properties;

namespace Raven.Migrator
{
    public static class MigrationHelpers
    {
        private const string RavenDocumentId = "@id";
        private const string RavenCollection = "@collection";

        private const string MetadataProperty = "@metadata";
        private const string MetadataPropertyFlags = "@flags";
        private const string MetadataPropertyFlagsValue = "HasAttachments";
        private const string MetadataPropertyAttachments = "@attachments";

        private const string AttachmentTagPrefix = "Attachment";
        private const string ExportTypeProperty = "@export-type";
        private const string ExportTypeValue = "Attachment";
        private const string HashProperty = "Hash";
        private const string SizeProperty = "Size";
        private const string TagProperty = "Tag";

        private const string AttachmentInfoName = "Name";
        private const string AttachmentInfoHash = "Hash";
        private const string AttachmentInfoContentType = "ContentType";
        private const string AttachmentInfoSize = "Size";

        private static readonly Lazy<JsonSerializer> JsonSerializer = new Lazy<JsonSerializer>(() => new JsonSerializer());

        private static readonly Lazy<IBlake2B> Blake2BFactory = new Lazy<IBlake2B>(() =>
            System.Data.HashFunction.Blake2.Blake2BFactory.Instance.Create(new Blake2BConfig
            {
                HashSizeInBits = 256
            }));

        public static void OutputClass(
            AbstractMigrationConfiguration configuration,
            object obj)
        {
            if (configuration.ConsoleExport)
            {
                var jsonString = JsonConvert.SerializeObject(obj);
                Console.WriteLine(jsonString);
                return;
            }

            foreach (var prop in obj.GetType().GetProperties())
            {
                var value = prop.GetValue(obj, null);
                if (value is List<string> list)
                {
                    value = string.Join(", ", list);
                }

                Console.WriteLine("{0}: {1}", prop.Name, value);
            }
        }

        public static async Task MigrateNoSqlDatabase(
            AbstractMigrationConfiguration configuration,
            Func<string, string, JsonTextWriter, StreamWriter, Task> migrateSingleCollection,
            Func<JsonTextWriter, StreamWriter, Task> migrateAttachments = null)
        {
            Stream stream;
            if (configuration.ConsoleExport)
            {
                stream = Console.OpenStandardOutput();
            }
            else
            {
                var fileName = $"Dump of {configuration.DatabaseName} {DateTime.UtcNow.ToString("yyyy-MM-dd HH-mm", CultureInfo.InvariantCulture)}.ravendbdump";
                var basePath = string.IsNullOrWhiteSpace(configuration.ExportFilePath) ? Directory.GetCurrentDirectory() : configuration.ExportFilePath;
                stream = File.Create(Path.Combine(basePath, fileName));
            }

            using (GetStream(configuration, stream, out var outStream))
            using (var streamWriter = new StreamWriter(outStream))
            using (var jsonTextWriter = new JsonTextWriter(streamWriter))
            {
                if (configuration.ConsoleExport == false)
                    jsonTextWriter.Formatting = Formatting.Indented;

                await jsonTextWriter.WriteStartObjectAsync();

                await jsonTextWriter.WritePropertyNameAsync("BuildVersion");
                await jsonTextWriter.WriteValueAsync(RavenVersionAttribute.Instance.BuildVersion);

                await jsonTextWriter.WritePropertyNameAsync("Docs");
                await jsonTextWriter.WriteStartArrayAsync();

                foreach (var collection in configuration.CollectionsToMigrate)
                {
                    var mongoCollectionName = collection.Name;
                    var ravenCollectionName = collection.NewName;
                    if (string.IsNullOrWhiteSpace(ravenCollectionName))
                        ravenCollectionName = mongoCollectionName;

                    await migrateSingleCollection(mongoCollectionName, ravenCollectionName, jsonTextWriter, streamWriter);
                }

                if (migrateAttachments != null)
                    await migrateAttachments(jsonTextWriter, streamWriter);

                await jsonTextWriter.WriteEndArrayAsync();
                await jsonTextWriter.WriteEndObjectAsync();
            }
        }

        private static IDisposable GetStream(
            AbstractMigrationConfiguration configuration,
            Stream stream, out Stream outStream)
        {
            if (configuration.ConsoleExport)
            {
                outStream = stream;
                return new DisposableAction(stream.Dispose);
            }

            var gzipStream = outStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true);
            return new DisposableAction(() =>
            {
                using (stream)
                using (gzipStream)
                {
                }
            });
        }

        public static void WriteDocument(
            ExpandoObject document,
            string documentId,
            string collectionName,
            List<string> propertiesToRemove,
            JsonTextWriter jsonTextWriter,
            List<Dictionary<string, object>> attachments = null)
        {
            var dictionary = (IDictionary<string, object>)document;
            if (propertiesToRemove != null)
            {
                foreach (var toRemove in propertiesToRemove)
                {
                    dictionary.Remove(toRemove);
                }
            }

            var metadata = new Dictionary<string, object>
            {
                {RavenDocumentId, documentId},
                {RavenCollection, collectionName}
            };

            if (attachments != null && attachments.Count > 0)
            {
                metadata.Add(MetadataPropertyFlags, MetadataPropertyFlagsValue);
                metadata.Add(MetadataPropertyAttachments, attachments);
            }

            dictionary[MetadataProperty] = metadata;
            JsonSerializer.Value.Serialize(jsonTextWriter, document);
        }

        public static async Task<Dictionary<string, object>> WriteAttachment(
            Stream attachmentStream, long totalSize, string attachmentId,
            string collectionName, string contentType, long attachmentNumber,
            JsonTextWriter jsonTextWriter, StreamWriter streamWriter)
        {
            var hash = Blake2BFactory.Value.ComputeHash(attachmentStream).AsBase64String();
            var tag = $"{AttachmentTagPrefix}{attachmentNumber}";
            await jsonTextWriter.WriteStartObjectAsync();
            await jsonTextWriter.WritePropertyNameAsync(MetadataProperty);
            await jsonTextWriter.WriteStartObjectAsync();
            await jsonTextWriter.WritePropertyNameAsync(ExportTypeProperty);
            await jsonTextWriter.WriteValueAsync(ExportTypeValue);
            await jsonTextWriter.WriteEndObjectAsync();
            await jsonTextWriter.WritePropertyNameAsync(HashProperty);
            await jsonTextWriter.WriteValueAsync(hash);
            await jsonTextWriter.WritePropertyNameAsync(SizeProperty);
            await jsonTextWriter.WriteValueAsync(totalSize);
            await jsonTextWriter.WritePropertyNameAsync(TagProperty);
            await jsonTextWriter.WriteValueAsync(tag);
            await jsonTextWriter.WriteEndObjectAsync();

            await jsonTextWriter.FlushAsync();
            await streamWriter.FlushAsync();

            attachmentStream.Position = 0;
            await attachmentStream.CopyToAsync(streamWriter.BaseStream);

            var attachmentInfo = new Dictionary<string, object>
            {
                {AttachmentInfoName, attachmentId},
                {AttachmentInfoHash, hash},
                {AttachmentInfoContentType, contentType},
                {AttachmentInfoSize, totalSize}
            };

            return attachmentInfo;
        }
    }
}

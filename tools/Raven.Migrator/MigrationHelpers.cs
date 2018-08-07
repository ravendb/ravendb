using System;
using System.Collections.Generic;
using System.Data.HashFunction.Blake2;
using System.Dynamic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Raven.Migrator
{
    public static class MigrationHelpers
    {
        private const string StartObject = "{";
        private const string EndObject = "}";
        private const string RavenDocumentId = "@id";
        private const string RavenCollection = "@collection";

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
            Func<string, string, Reference<bool>, StreamWriter, Task> migrateSingleCollection,
            Func<Reference<bool>, StreamWriter, Task> migrateAttachments = null)
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

            using (stream)
            using (var gzipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
            using (var streamWriter = new StreamWriter(gzipStream))
            {
                await streamWriter.WriteAsync("{\"Docs\":[");

                var isFirstDocument = new Reference<bool> { Value = true };
                foreach (var collection in configuration.CollectionsToMigrate)
                {
                    var mongoCollectionName = collection.Name;
                    var ravenCollectionName = collection.NewName;
                    if (string.IsNullOrWhiteSpace(ravenCollectionName))
                        ravenCollectionName = mongoCollectionName;

                    await migrateSingleCollection(mongoCollectionName, ravenCollectionName, isFirstDocument, streamWriter);
                }

                if (migrateAttachments != null)
                    await migrateAttachments(isFirstDocument, streamWriter);

                await streamWriter.WriteAsync("]}");
            }
        }

        public static async Task WriteDocument(
            ExpandoObject document,
            string documentId,
            string collectionName,
            List<string> propertiesToRemove,
            Reference<bool> isFirstDocument,
            StreamWriter streamWriter,
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

            if (attachments == null || attachments.Count == 0)
            {
                dictionary["@metadata"] = new Dictionary<string, object>
                {
                    {RavenDocumentId, documentId},
                    {RavenCollection, collectionName}
                };
            }
            else
            {
                dictionary["@metadata"] = new Dictionary<string, object>
                {
                    {RavenDocumentId, documentId},
                    {RavenCollection, collectionName},
                    {"@flags", "HasAttachments"},
                    {"@attachments", attachments}
                };
            }

            if (isFirstDocument.Value == false)
                await streamWriter.WriteAsync(",");
            isFirstDocument.Value = false;

            var jsonString = JsonConvert.SerializeObject(document);
            await streamWriter.WriteAsync(jsonString);
        }

        public static async Task<Dictionary<string, object>> WriteAttachment( 
            Stream attachmentStream, long totalSize, string attachmentId, 
            string collectionName, string contentType, long attachmentNumber,
            Reference<bool> isFirstDocument, StreamWriter streamWriter)
        {
            if (isFirstDocument.Value == false)
                await streamWriter.WriteAsync(",");
            isFirstDocument.Value = false;

            var hash = Blake2BFactory.Value.ComputeHash(attachmentStream).AsBase64String();
            var tag = $"Attachment{attachmentNumber}";
            await streamWriter.WriteAsync(
                $"{StartObject}{GetQuotedString("@metadata")}:{StartObject}" +
                $"{GetQuotedString("@export-type")}:{GetQuotedString("Attachment")}{EndObject}," +
                $"{GetQuotedString("Hash")}:{GetQuotedString(hash)},{GetQuotedString("Size")}:{totalSize}," +
                $"{GetQuotedString("Tag")}:{GetQuotedString(tag)}{EndObject}");

            await streamWriter.FlushAsync();
            attachmentStream.Position = 0;
            await attachmentStream.CopyToAsync(streamWriter.BaseStream);

            var attachmentInfo = new Dictionary<string, object>
            {
                {"Name", attachmentId},
                {"Hash", hash},
                {"ContentType", contentType},
                {"Size", totalSize}
            };

            return attachmentInfo;
        }

        private static string GetQuotedString(string name)
        {
            return $"\"{name}\"";
        }
    }
}

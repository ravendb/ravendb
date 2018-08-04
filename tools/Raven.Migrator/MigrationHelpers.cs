using System;
using System.Collections.Generic;
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

        public static void OutputClass(
            AbstractMigrationConfiguration configuration,
            object obj)
        {
            if (configuration.ConsoleExport)
            {
                foreach (var prop in obj.GetType().GetProperties())
                {
                    var value = prop.GetValue(obj, null);
                    if (value is List<string> list)
                    {
                        value = string.Join(", ", list);
                    }

                    Console.WriteLine("{0}: {1}", prop.Name, value);
                }
                return;
            }

            var jsonString = JsonConvert.SerializeObject(obj);
            Console.WriteLine(jsonString);
        }

        public static IDisposable GetStreamWriter(AbstractMigrationConfiguration configuration, out StreamWriter streamWriter)
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

            var gzipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true);
            var streamWriterInternal = streamWriter = new StreamWriter(gzipStream);

            return new DisposableAction(() =>
            {
                using (stream)
                using (gzipStream)
                using (streamWriterInternal)
                {

                }
            });
        }

        internal class DisposableAction : IDisposable
        {
            private readonly Action _action;

            public DisposableAction(Action action)
            {
                _action = action;
            }

            public void Dispose()
            {
                _action();
            }
        }

        public static async Task WriteDocument(
            ExpandoObject document,
            string databaseDocumentId,
            string collectionName,
            List<string> propertiesToRemove,
            Reference<bool> isFirstDocument,
            StreamWriter streamWriter)
        {
            var dictionary = (IDictionary<string, object>)document;
            var documentKey = dictionary[databaseDocumentId].ToString();
            foreach (var toRemove in propertiesToRemove)
            {
                dictionary.Remove(toRemove);
            }

            dictionary["@metadata"] = new Dictionary<string, object>
            {
                {RavenDocumentId, documentKey},
                {"@collection", collectionName}
            };

            if (isFirstDocument.Value == false)
                await streamWriter.WriteAsync(",");
            isFirstDocument.Value = false;

            var jsonString = JsonConvert.SerializeObject(document);
            await streamWriter.WriteAsync(jsonString);
        }

        public static async Task WriteDocumentWithAttachment( 
            ExpandoObject document, Stream attachmentStream, long totalSize, 
            string documentId, string collectionName, string contentType, long attachmentNumber,
            Reference<bool> isFirstDocument, StreamWriter streamWriter)
        {
            if (isFirstDocument.Value == false)
                await streamWriter.WriteAsync(",");
            isFirstDocument.Value = false;

            // we are going to recalculate the hash when importing
            var hash = string.Empty;

            var tag = $"GridFS{attachmentNumber}";
            await streamWriter.WriteAsync(
                $"{StartObject}{GetQuotedString("@metadata")}:{StartObject}" +
                $"{GetQuotedString("@export-type")}:{GetQuotedString("Attachment")}{EndObject}," +
                $"{GetQuotedString("Hash")}:{GetQuotedString(hash)},{GetQuotedString("Size")}:{totalSize}," +
                $"{GetQuotedString("Tag")}:{GetQuotedString(tag)}{EndObject}");

            await streamWriter.FlushAsync();
            attachmentStream.Position = 0;
            await attachmentStream.CopyToAsync(streamWriter.BaseStream);
            await streamWriter.WriteAsync(",");

            // write the dummy document
            var dictionary = (IDictionary<string, object>)document;
            //dictionary.Remove(databaseDocumentId);
            var attachmentInfo = new Dictionary<string, object>
            {
                {"Name", documentId},
                {"Hash", hash},
                {"ContentType", contentType},
                {"Size", totalSize}
            };
            var attachments = new List<Dictionary<string, object>>
            {
                {attachmentInfo}
            };

            dictionary["@metadata"] = new Dictionary<string, object>
            {
                {"@id", documentId},
                {"@collection", collectionName},
                {"@flags", "HasAttachments"},
                {"@attachments", attachments}
            };

            var jsonString = JsonConvert.SerializeObject(document);
            await streamWriter.WriteAsync(jsonString);
        }

        private static string GetQuotedString(string name)
        {
            return $"\"{name}\"";
        }
    }
}

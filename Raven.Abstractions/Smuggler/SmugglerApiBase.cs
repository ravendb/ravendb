using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Abstractions.Smuggler
{
    public abstract class SmugglerApiBase : ISmugglerApi
    {
        protected bool ensuredDatabaseExists;

        public SmugglerStats ImportData(SmugglerOptions options)
        {
            using (FileStream fileStream = File.OpenRead(options.File))
            {
                return ImportData(fileStream, options);
            }
        }

        public SmugglerStats ImportData(Stream stream, SmugglerOptions options)
        {
            EnsureDatabaseExists();

            var sw = Stopwatch.StartNew();
            var stats = new SmugglerStats();

            // Try to read the stream compressed, otherwise continue uncompressed.
            JsonTextReader jsonReader;
            try
            {
                var streamReader = new StreamReader(new GZipStream(stream, CompressionMode.Decompress));
                jsonReader = new JsonTextReader(streamReader);
                if (jsonReader.Read() == false)
                    return stats;
            }
            catch (InvalidDataException)
            {
                stream.Seek(0, SeekOrigin.Begin);
                var streamReader = new StreamReader(stream);
                jsonReader = new JsonTextReader(streamReader);
                if (jsonReader.Read() == false)
                    return stats;
            }

            if (jsonReader.TokenType != JsonToken.StartObject)
                throw new InvalidDataException("StartObject was expected");

            // should read indexes now
            if (jsonReader.Read() == false)
                return stats;
            if (jsonReader.TokenType != JsonToken.PropertyName)
                throw new InvalidDataException("PropertyName was expected");
            if (Equals("Indexes", jsonReader.Value) == false)
                throw new InvalidDataException("Indexes property was expected");
            if (jsonReader.Read() == false)
                return stats;
            if (jsonReader.TokenType != JsonToken.StartArray)
                throw new InvalidDataException("StartArray was expected");

            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
            {
                var index = RavenJToken.ReadFrom(jsonReader);
                if (options.OperateOnTypes.HasFlag(ItemType.Indexes) == false)
                    continue;
                var indexName = index.Value<string>("name");
                if (indexName.StartsWith("Raven/") || indexName.StartsWith("Temp/"))
                    continue;
                PutIndex(indexName, index);
                stats.Indexes++;
            }

            // should read documents now
            if (jsonReader.Read() == false)
                return stats;
            if (jsonReader.TokenType != JsonToken.PropertyName)
                throw new InvalidDataException("PropertyName was expected");
            if (Equals("Docs", jsonReader.Value) == false)
                throw new InvalidDataException("Docs property was expected");
            if (jsonReader.Read() == false)
                return stats;
            if (jsonReader.TokenType != JsonToken.StartArray)
                throw new InvalidDataException("StartArray was expected");
            var batch = new List<RavenJObject>();
            int totalCount = 0;
            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
            {
                var document = (RavenJObject)RavenJToken.ReadFrom(jsonReader);
                if (options.OperateOnTypes.HasFlag(ItemType.Documents) == false)
                    continue;
                if (options.MatchFilters(document) == false)
                    continue;

                totalCount += 1;
                batch.Add(document);
                stats.Documents++;
                if (batch.Count >= 128)
                    FlushBatch(batch);
            }
            FlushBatch(batch);

            var attachmentCount = 0;
            if (jsonReader.Read() == false || jsonReader.TokenType == JsonToken.EndObject)
                return stats;
            if (jsonReader.TokenType != JsonToken.PropertyName)
                throw new InvalidDataException("PropertyName was expected");
            if (Equals("Attachments", jsonReader.Value) == false)
                throw new InvalidDataException("Attachment property was expected");
            if (jsonReader.Read() == false)
                return stats;
            if (jsonReader.TokenType != JsonToken.StartArray)
                throw new InvalidDataException("StartArray was expected");
            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
            {
                attachmentCount += 1;
                var item = RavenJToken.ReadFrom(jsonReader);
                if (options.OperateOnTypes.HasFlag(ItemType.Attachments) == false)
                    continue;
                var attachmentExportInfo =
                    new JsonSerializer
                        {
                            Converters = { new TrivialJsonToJsonJsonConverter() }
                        }.Deserialize<AttachmentExportInfo>(new RavenJTokenReader(item));
                ShowProgress("Importing attachment {0}", attachmentExportInfo.Key);

                PutAttachment(attachmentExportInfo);
                stats.Attachments++;
            }

            sw.Stop();

            ShowProgress("Imported {0:#,#;;0} documents and {1:#,#;;0} attachments in {2:#,#;;0} ms", totalCount, attachmentCount, sw.ElapsedMilliseconds);
            stats.Elapsed = sw.Elapsed;
            return stats;
        }

        protected abstract void PutAttachment(AttachmentExportInfo attachmentExportInfo);
        protected abstract void PutIndex(string indexName, RavenJToken index);

        public void ExportData(SmugglerOptions options)
        {
            using (var streamWriter = new StreamWriter(new GZipStream(File.Create(options.File), CompressionMode.Compress)))
            {
                var jsonWriter = new JsonTextWriter(streamWriter)
                                     {
                                         Formatting = Formatting.Indented
                                     };
                jsonWriter.WriteStartObject();
                jsonWriter.WritePropertyName("Indexes");
                jsonWriter.WriteStartArray();
                if (options.OperateOnTypes.HasFlag(ItemType.Indexes))
                {
                    ExportIndexes(jsonWriter);
                }
                jsonWriter.WriteEndArray();

                jsonWriter.WritePropertyName("Docs");
                jsonWriter.WriteStartArray();
                if (options.OperateOnTypes.HasFlag(ItemType.Documents))
                {
                    ExportDocuments(options, jsonWriter);
                }
                jsonWriter.WriteEndArray();

                jsonWriter.WritePropertyName("Attachments");
                jsonWriter.WriteStartArray();
                if (options.OperateOnTypes.HasFlag(ItemType.Attachments))
                {
                    ExportAttachments(jsonWriter);
                }
                jsonWriter.WriteEndArray();

                jsonWriter.WriteEndObject();
                streamWriter.Flush();
            }
        }


        protected abstract void ShowProgress(string format, params object[] args);

        /// <summary>
        /// Left as fully abstract as the access to attachments is quite different from database and http.
        /// SmugglerApi uses ETags and two HTTP calls to retrieve attachments.
        /// Database uses an integer count and the AttachmentInformation structure to retrieve attachments.
        /// Not sure I'm the one to decide if these need to be refactored.
        /// </summary>
        /// <param name="jsonWriter"></param>
        protected abstract void ExportAttachments(JsonTextWriter jsonWriter);

        protected void ExportDocuments(SmugglerOptions options, JsonTextWriter jsonWriter)
        {
            var lastEtag = Guid.Empty;
            int totalCount = 0;
            while (true)
            {
                RavenJArray documents = GetDocuments(options, lastEtag);
                if (documents.Length == 0)
                {
                    ShowProgress("Done with reading documents, total: {0}", totalCount);
                    break;
                }

                var final = documents.Where(options.MatchFilters).ToList();
                final.ForEach(item => item.WriteTo(jsonWriter));
                totalCount += final.Count;

                ShowProgress("Reading batch of {0,3} documents, read so far: {1,10:#,#;;0}", documents.Length, totalCount);
                lastEtag = new Guid(documents.Last().Value<RavenJObject>("@metadata").Value<string>("@etag"));
            }
        }

        protected void ExportIndexes(JsonTextWriter jsonWriter)
        {
            int totalCount = 0;
            while (true)
            {
                RavenJArray indexes = GetIndexes(totalCount);
                if (indexes.Length == 0)
                {
                    ShowProgress("Done with reading indexes, total: {0}", totalCount);
                    break;
                }
                totalCount += indexes.Length;
                ShowProgress("Reading batch of {0,3} indexes, read so far: {1,10:#,#;;0}", indexes.Length, totalCount);
                foreach (RavenJToken item in indexes)
                {
                    item.WriteTo(jsonWriter);
                }
            }
        }



        protected abstract RavenJArray GetDocuments(SmugglerOptions options, Guid lastEtag);
        protected abstract RavenJArray GetIndexes(int totalCount);


        protected static string StripQuotesIfNeeded(RavenJToken value)
        {
            var str = value.ToString(Formatting.None);
            if (str.StartsWith("\"") && str.EndsWith("\""))
                return str.Substring(1, str.Length - 2);
            return str;
        }

        protected class AttachmentExportInfo
        {
            public byte[] Data { get; set; }
            public RavenJObject Metadata { get; set; }
            public string Key { get; set; }
        }

        protected abstract void EnsureDatabaseExists();
        protected abstract void FlushBatch(List<RavenJObject> batch);
    }
}
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Smuggler
{
    public class RepairApi : SmugglerApiBase
    {
        private StreamWriter exporter;
        private JsonTextWriter jsonExporter;
        private bool hasSeenAttachments;
        private bool hasSeenIndexes;
        private bool hasSeenDocs;
        private Dictionary<string, RavenJObject> docs;

        public RepairApi(SmugglerOptions smugglerOptions) 
            : base(smugglerOptions)
        {            
        }

        protected override RavenJArray GetIndexes(int totalCount)
        {
            return new RavenJArray();
        }

        protected override RavenJArray GetDocuments(Guid lastEtag)
        {
            return new RavenJArray();
        }

        protected override Guid ExportAttachments(JsonTextWriter jsonWriter, Guid lastEtag)
        {
            return new Guid();
        }

        protected override void PutIndex(string indexName, RavenJToken index)
        {
            if (!hasSeenIndexes)
            {
                if (HasSeenAnything)
                    jsonExporter.WriteEndArray();
                jsonExporter.WritePropertyName("Indexes");
                jsonExporter.WriteStartArray();

                hasSeenIndexes = true;
            }

            if ((smugglerOptions.OperateOnTypes & ItemType.Indexes) == ItemType.Indexes)
            {
                
            }
        }

        protected override void PutAttachment(AttachmentExportInfo attachmentExportInfo)
        {
            if (!hasSeenAttachments)
            {
                PutDocuments();

                if (HasSeenAnything)
                    jsonExporter.WriteEndArray();
                jsonExporter.WritePropertyName("Attachments");
                jsonExporter.WriteStartArray();

                hasSeenAttachments = true;
            }

            if ((smugglerOptions.OperateOnTypes & ItemType.Attachments) == ItemType.Attachments)
            {
                new RavenJObject
                {
                    {"Data", attachmentExportInfo.Data},
                    {"Metadata", attachmentExportInfo.Metadata},
                    {"Key", attachmentExportInfo.Key}
                }.WriteTo(jsonExporter);
            }
        }

        protected override Guid FlushBatch(List<RavenJObject> batch)
        {
            if ((smugglerOptions.OperateOnTypes & ItemType.Documents) == ItemType.Documents)
            {
                foreach (var doc in batch)
                {
                    var metadata = (RavenJObject)doc["@metadata"];

                    var id = metadata.Value<string>("@id");
                    if (id.Contains("/conflicts/"))
                    {
                        id = id.Substring(0, id.IndexOf('/'));
                    }
                    metadata["@id"] = id;

                    // clear replication data
                    foreach (var key in metadata.Keys)
                    {
                        if (key.StartsWith("Raven-Replication"))
                            metadata.Remove(key);
                    }
                    doc["@metadata"] = metadata;
                    

                    RavenJObject conflict;
                    if (!docs.TryGetValue(id, out conflict))
                    {
                        docs.Add(id, doc);
                    }
                    else
                    {
                        var lastModified = metadata.Value<DateTime>("Last-Modified");

                        var conflictMetadata = conflict["@metadata"];
                        var conflictLastModified = conflictMetadata.Value<DateTime>("Last-Modified");
                        if (lastModified > conflictLastModified)
                            docs[id] = doc;
                    }                                            
                }
            }

            return new Guid();
        }

        public override void ImportData(SmugglerOptions options, bool incremental = false)
        {
            if (exporter != null)
                throw new InvalidOperationException("Cannot repair more than one dump at a time.");

            var file = options.BackupPath + ".repair";
            exporter = new StreamWriter(new GZipStream(File.Create(file), CompressionMode.Compress));
            docs = new Dictionary<string, RavenJObject>();
            try
            {
                jsonExporter = new JsonTextWriter(exporter)
                {
                    Formatting = Formatting.Indented
                };
                jsonExporter.WriteStartObject();
                
                base.ImportData(options, incremental);

                // just in case we do not have attachments
                PutDocuments();

                if (HasSeenAnything)
                    jsonExporter.WriteEndArray();

                jsonExporter.WriteEndObject();
            }
            finally
            {
                exporter.Flush();
                exporter.Dispose();
                exporter = null;
            }                        
        }

        private void PutDocuments()
        {
            if (hasSeenDocs)
                return;

            if (HasSeenAnything)
                jsonExporter.WriteEndArray();

            jsonExporter.WritePropertyName("Docs");
            jsonExporter.WriteStartArray();

            hasSeenDocs = true;

            foreach (var doc in docs.Values)
            {
                doc.WriteTo(jsonExporter);
            }                        
        }

        protected override DatabaseStatistics GetStats()
        {
            return new DatabaseStatistics
            {
                ActualIndexingBatchSize = new ActualIndexingBatchSize[0],
                Errors = new ServerError[0],
                Indexes = new IndexStats[0],
                StaleIndexes = new string[0],
                Triggers = new DatabaseStatistics.TriggerInfo[0],
                Prefetches = new FutureBatchStats[0]
            };
        }

        protected override void ShowProgress(string format, params object[] args)
        {
            Console.WriteLine(format, args);
        }

        protected override void EnsureDatabaseExists() { }

        private bool HasSeenAnything
        {
            get { return hasSeenAttachments || hasSeenDocs || hasSeenIndexes; }
        }

    }
}

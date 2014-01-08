using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Smuggler
{
    public class DumpApi : SmugglerApiBase
    {
        public DumpApi(SmugglerOptions smugglerOptions) : base(smugglerOptions)
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
            return lastEtag;
        }

        protected override void PutIndex(string indexName, RavenJToken index)
        {
            Console.Error.WriteLine("PUT INDEX '{0}'", indexName);
        }

        protected override void PutAttachment(AttachmentExportInfo attachmentExportInfo)
        {
            Console.Error.WriteLine("PUT ATTACHMENT '{0}'", attachmentExportInfo.Key);
        }

        protected override Guid FlushBatch(List<RavenJObject> batch)
        {
            foreach (var doc in batch)
            {
                var id = doc["@metadata"].Value<string>("@id");
                Console.Error.WriteLine("PUT DOCUMENT '{0}'", id);
            }
            return new Guid();
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
    }
}

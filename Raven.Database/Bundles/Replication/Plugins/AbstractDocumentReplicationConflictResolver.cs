using System;
using System.ComponentModel.Composition;
using System.Text;

using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Plugins
{
    [InheritedExport]
    public abstract class AbstractDocumentReplicationConflictResolver
    {
        private readonly ILog log = LogManager.GetCurrentClassLogger();

        public bool TryResolveConflict(string id, RavenJObject metadata, RavenJObject document, JsonDocument existingDoc,
                                        Func<string, JsonDocument> getDocument, out RavenJObject metadataToSave,
                                        out RavenJObject documentToSave)
        {
            var success = TryResolve(id, metadata, document, existingDoc, getDocument, out metadataToSave, out documentToSave);
            if (success == false)
                return false;

            // here we make sure that we keep a deleted document deleted, rather than "reviving" it.
            //var ravenDeleteMarker = existingDoc.Metadata.Value<string>("Raven-Delete-Marker");
            //bool markerValue;
            //if (ravenDeleteMarker != null && bool.TryParse(ravenDeleteMarker, out markerValue) && markerValue)
            //{
            //    existingDoc.Metadata.Add("Raven-Remove-Document-Marker", true);
            //}

            var docToSave = documentToSave;
            var metaToSave = metadataToSave;
            log.Debug(() =>
            {
                var builder = new StringBuilder();
                builder.AppendLine(string.Format("Conflict on document with key '{0}' resolved by '{1}'.", id, GetType().Name));
                builder.AppendLine(string.Format("Existing document:"));
                if (existingDoc != null && existingDoc.DataAsJson != null)
                    builder.AppendLine(existingDoc.DataAsJson.ToString());
                builder.AppendLine(string.Format("Existing metadata:"));
                if (existingDoc != null && existingDoc.Metadata != null)
                    builder.AppendLine(existingDoc.Metadata.ToString());
                builder.AppendLine(string.Format("Incoming document:"));
                if (document != null)
                    builder.AppendLine(document.ToString());
                builder.AppendLine(string.Format("Incoming metadata:"));
                if (metadata != null)
                    builder.AppendLine(metadata.ToString());
                builder.AppendLine(string.Format("Output document:"));
                if (docToSave != null)
                    builder.AppendLine(docToSave.ToString());
                builder.AppendLine(string.Format("Output metadata:"));
                if (metaToSave != null)
                    builder.AppendLine(metaToSave.ToString());

                return builder.ToString();
            });

            return true;
        }

        protected abstract bool TryResolve(string id, RavenJObject metadata, RavenJObject document, JsonDocument existingDoc,
                                        Func<string, JsonDocument> getDocument, out RavenJObject metadataToSave,
                                        out RavenJObject documentToSave);
    }
}

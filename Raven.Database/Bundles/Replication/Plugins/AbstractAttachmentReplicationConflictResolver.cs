using System;
using System.ComponentModel.Composition;
using System.Text;

using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Plugins
{
    [InheritedExport]
    [Obsolete("Use RavenFS instead.")]
    public abstract class AbstractAttachmentReplicationConflictResolver
    {
        private readonly ILog log = LogManager.GetCurrentClassLogger();

        public bool TryResolveConflict(string id, RavenJObject metadata, byte[] data, Attachment existingAttachment,
                                        Func<string, Attachment> getAttachment, out RavenJObject metadataToSave,
                                        out byte[] dataToSave)
        {
            var success = TryResolve(id, metadata, data, existingAttachment, getAttachment, out metadataToSave, out dataToSave);
            if (success == false)
                return false;

            // here we make sure that we keep a deleted attachment deleted, rather than "reviving" it.
            var ravenDeleteMarker = existingAttachment.Metadata.Value<string>("Raven-Delete-Marker");
            bool markerValue;
            if (ravenDeleteMarker != null && bool.TryParse(ravenDeleteMarker, out markerValue) && markerValue)
            {
                existingAttachment.Metadata["Raven-Remove-Document-Marker"] = true;
            }

            var metaToSave = metadataToSave;
            if (log.IsDebugEnabled)
                log.Debug(() =>
                {
                    var builder = new StringBuilder();
                    builder.AppendLine(string.Format("Conflict on attachment with key '{0}' resolved by '{1}'.", id, GetType().Name));
                    builder.AppendLine(string.Format("Existing metadata:"));
                    if (existingAttachment != null && existingAttachment.Metadata != null)
                        builder.AppendLine(existingAttachment.Metadata.ToString());
                    builder.AppendLine(string.Format("Incoming metadata:"));
                    if (metadata != null)
                        builder.AppendLine(metadata.ToString());
                    builder.AppendLine(string.Format("Output metadata:"));
                    if (metaToSave != null)
                        builder.AppendLine(metaToSave.ToString());

                    return builder.ToString();
                });

            return true;
        }

        protected abstract bool TryResolve(string id, RavenJObject metadata, byte[] data, Attachment existingAttachment,
                                        Func<string, Attachment> getAttachment, out RavenJObject metadataToSave,
                                        out byte[] dataToSave);
    }
}

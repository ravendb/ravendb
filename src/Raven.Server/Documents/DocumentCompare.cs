using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents
{
    public static class DocumentCompare
    {
        public readonly struct DocumentCompareOptions
        {
            private DocumentCompareOptions(bool tryMergeMetadataConflicts, bool throwOnAttachmentModifications, bool compareDataArchivalMetadata = false)
            {
                TryMergeMetadataConflicts = tryMergeMetadataConflicts;
                ThrowOnAttachmentModifications = throwOnAttachmentModifications;
                CompareDataArchivalMetadata = compareDataArchivalMetadata;
            }

            public readonly bool TryMergeMetadataConflicts;
            public readonly bool ThrowOnAttachmentModifications;
            public readonly bool CompareDataArchivalMetadata;

            public static DocumentCompareOptions Default = new DocumentCompareOptions();

            public static DocumentCompareOptions MergeMetadata =
                new DocumentCompareOptions(tryMergeMetadataConflicts: true, throwOnAttachmentModifications: false);

            public static DocumentCompareOptions MergeMetadataAndThrowOnAttachmentModification =
                new DocumentCompareOptions(tryMergeMetadataConflicts: true, throwOnAttachmentModifications: true);
            
            public static DocumentCompareOptions  MergeMetadataAndThrowOnAttachmentModificationCompareDataArchivalMetadata =
                new DocumentCompareOptions(tryMergeMetadataConflicts: true, throwOnAttachmentModifications: true, compareDataArchivalMetadata: true);
        }

        public static unsafe DocumentCompareResult IsEqualTo(BlittableJsonReaderObject original, BlittableJsonReaderObject modified, in DocumentCompareOptions options)
        {
            if (ReferenceEquals(original, modified))
                return DocumentCompareResult.Equal;

            if (original == null || modified == null)
                return DocumentCompareResult.NotEqual;

            BlittableJsonReaderObject.AssertNoModifications(original, nameof(original), true);
            BlittableJsonReaderObject.AssertNoModifications(modified, nameof(modified), true);

            if (original.Size == modified.Size)
            {
                // if this didn't change, we can check the raw memory directly.
                if (Memory.Compare(original.BasePointer, modified.BasePointer, original.Size) == 0)
                    return DocumentCompareResult.Equal;
            }

            // Performance improvement: We compare the metadata first 
            // because that most of the time the metadata itself won't be the equal, so no need to compare all values

            var result = IsMetadataEqualTo(original, modified, options);
            if (result == DocumentCompareResult.NotEqual)
                return DocumentCompareResult.NotEqual;

            if (AreBodiesEqual(original, modified) == false)
                return DocumentCompareResult.NotEqual;

            return result;
        }

        private static bool AreBodiesEqual(BlittableJsonReaderObject current, BlittableJsonReaderObject modified)
        {
            for (int i = 0; i < current.Count; i++)
            {
                var name = current.GetRawPropertyNameByIndex(i);
                if (name.SequenceEqual("@metadata"u8))
                    continue;

                int modifiedIndex = modified.GetPropertyIndex(name);
                if (modifiedIndex == -1 || Equals(current.GetValueByIndex(i), modified.GetValueByIndex(modifiedIndex)) == false)
                    return false;
            }

            for (int i = 0; i < modified.Count; i++)
            {
                var name = modified.GetRawPropertyNameByIndex(i);
                if (name.SequenceEqual("@metadata"u8) == false && current.GetPropertyIndex(name) == -1)
                    return false;
            }

            return true;
        }

        private static DocumentCompareResult IsMetadataEqualTo(BlittableJsonReaderObject current, BlittableJsonReaderObject modified, in DocumentCompareOptions options)
        {
            if (modified == null)
                return DocumentCompareResult.NotEqual;

            current.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject currentMetadata);
            modified.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject objMetadata);

            if (currentMetadata == null && objMetadata == null)
                return DocumentCompareResult.Equal;

            if (currentMetadata == null || objMetadata == null)
            {
                if (options.TryMergeMetadataConflicts)
                {
                    DocumentCompareResult result = DocumentCompareResult.Equal;

                    if (currentMetadata == null)
                        currentMetadata = objMetadata;

                    // If there is a conflict on @metadata with @counters and/or with @attachments, we know how to resolve it.
                    var propertyNames = currentMetadata.GetPropertyNames();
                    if (propertyNames.Contains(Constants.Documents.Metadata.Counters, StringComparer.OrdinalIgnoreCase))
                        result |= DocumentCompareResult.CountersNotEqual;

                    if (propertyNames.Contains(Constants.Documents.Metadata.TimeSeries, StringComparer.OrdinalIgnoreCase))
                        result |= DocumentCompareResult.TimeSeriesNotEqual;

                    if (propertyNames.Contains(Constants.Documents.Metadata.Attachments, StringComparer.OrdinalIgnoreCase))
                    {
                        if (options.ThrowOnAttachmentModifications)
                        {
                            ThrowAttachmentsModificationsDetected();
                        }
                        result |= DocumentCompareResult.AttachmentsNotEqual;
                    }

                    return result != DocumentCompareResult.Equal ? result : DocumentCompareResult.NotEqual;
                }

                return DocumentCompareResult.NotEqual;
            }

            return CompareMetadataProperties(currentMetadata, objMetadata, options);
        }

        [DoesNotReturn]
        private static void ThrowAttachmentsModificationsDetected()
        {
            throw new InvalidOperationException("Illegal modifications of '@attachments' detected");
        }
        
        private static DocumentCompareResult CompareMetadataProperties(BlittableJsonReaderObject current, BlittableJsonReaderObject modified, in DocumentCompareOptions options)
        {
            var resolvedAttachmentConflict = false;
            var resolvedCountersConflict = false;
            var resolvedTimeSeriesConflict = false;

            // the properties of the current metadata, then the ones only the modified metadata has
            for (int pass = 0; pass < 2; pass++)
            {
                var source = pass == 0 ? current : modified;

                for (int index = 0; index < source.Count; index++)
                {
                    var name = source.GetRawPropertyNameByIndex(index);

                    int currentIndex = -1, modifiedIndex = -1;
                    if (pass == 0)
                    {
                        currentIndex = index;
                        modifiedIndex = modified.GetPropertyIndex(name);
                    }
                    else if (current.GetPropertyIndex(name) == -1)
                    {
                        modifiedIndex = index;
                    }
                    else
                    {
                        continue; // visited in the first pass
                    }

                    var inBoth = currentIndex != -1 && modifiedIndex != -1;

                    if (name.Length > 0 && name[0] == '@')
                    {
                        if (name.SequenceEqual("@attachments"u8))
                        {
                            if (options.TryMergeMetadataConflicts)
                            {
                                if (inBoth == false)
                                {
                                    // Resolve when just 1 document have attachments
                                    resolvedAttachmentConflict = true;
                                    continue;
                                }

                                resolvedAttachmentConflict = ShouldResolveAttachmentsConflict(current, modified, options);
                                if (resolvedAttachmentConflict)
                                    continue;

                                if (options.ThrowOnAttachmentModifications)
                                {
                                    ThrowAttachmentsModificationsDetected();
                                }
                                return DocumentCompareResult.NotEqual;
                            }
                        }
                        else if (name.SequenceEqual("@counters"u8))
                        {
                            if (options.TryMergeMetadataConflicts)
                            {
                                if (inBoth == false)
                                {
                                    // Resolve when just 1 document have counters
                                    resolvedCountersConflict = true;
                                    continue;
                                }

                                resolvedCountersConflict = ShouldResolveCountersConflict(current, modified);
                                continue;
                            }
                        }
                        else if (name.SequenceEqual("@timeseries"u8))
                        {
                            if (options.TryMergeMetadataConflicts)
                            {
                                if (inBoth == false)
                                {
                                    // Resolve when just 1 document have time-series
                                    resolvedTimeSeriesConflict = true;
                                    continue;
                                }

                                resolvedTimeSeriesConflict = ShouldResolveTimeSeriesConflict(current, modified);
                                continue;
                            }
                        }
                        else if (IsSignificantMetadataProperty(name, options) == false)
                            continue;
                    }

                    if (inBoth == false)
                        return DocumentCompareResult.NotEqual;

                    if (Equals(current.GetValueByIndex(currentIndex), modified.GetValueByIndex(modifiedIndex)) == false)
                        return DocumentCompareResult.NotEqual;
                }
            }

            var shouldRecreateAttachment = resolvedAttachmentConflict ? DocumentCompareResult.AttachmentsNotEqual : DocumentCompareResult.None;
            var shouldRecreateCounters = resolvedCountersConflict ? DocumentCompareResult.CountersNotEqual : DocumentCompareResult.None;
            var shouldRecreateTimeSeries = resolvedTimeSeriesConflict ? DocumentCompareResult.TimeSeriesNotEqual : DocumentCompareResult.None;

            return DocumentCompareResult.Equal | shouldRecreateAttachment | shouldRecreateCounters | shouldRecreateTimeSeries;
        }

        private static bool IsSignificantMetadataProperty(ReadOnlySpan<byte> name, in DocumentCompareOptions options)
        {
            if (name.SequenceEqual("@collection"u8) ||
                name.SequenceEqual("@expires"u8) ||
                name.SequenceEqual("@refresh"u8))
            {
                return true;
            }

            // Archival properties are significant only when the option is enabled
            if (options.CompareDataArchivalMetadata &&
                (name.SequenceEqual("@archive-at"u8) ||
                 name.SequenceEqual("@archived"u8)))
            {
                return true;
            }

            return false;
        }

        private static bool ShouldResolveAttachmentsConflict(BlittableJsonReaderObject currentMetadata, BlittableJsonReaderObject modifiedMetadata, in DocumentCompareOptions options)
        {
            currentMetadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray currentAttachments);
            modifiedMetadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray modifiedAttachments);
            Debug.Assert(currentAttachments != null || modifiedAttachments != null, "Cannot happen. We verified that we have a conflict in @attachments.");

            var currentAttachmentNames = new Dictionary<string, BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase);
            if (currentAttachments != null)
            {
                foreach (BlittableJsonReaderObject attachment in currentAttachments)
                {
                    if (attachment.TryGet(nameof(AttachmentName.Name), out string name) == false)
                        return false;   // Attachment must have a name. The user modified the value?

                    if (currentAttachmentNames.ContainsKey(name))
                        // The node itself has a conflict
                        return false;
                    currentAttachmentNames.Add(name, attachment);
                }
            }

            var modifiedAttachmentNames = new Dictionary<string, BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase);
            if (modifiedAttachments != null)
            {
                foreach (BlittableJsonReaderObject attachment in modifiedAttachments)
                {
                    if (attachment.TryGet(nameof(AttachmentName.Name), out string name) == false)
                        return false;   // Attachment must have a name. The user modified the value?

                    if (modifiedAttachmentNames.ContainsKey(name))
                        // The node itself has a conflict
                        return false;
                    modifiedAttachmentNames.Add(name, attachment);
                }
            }

            foreach (var attachment in currentAttachmentNames)
            {
                if (modifiedAttachmentNames.TryGetValue(attachment.Key, out var modifiedAttachment))
                {
                    if (AreBodiesEqual(attachment.Value, modifiedAttachment) == false)
                        return false;

                    modifiedAttachmentNames.Remove(attachment.Key);
                }
                else
                {
                    if (options.ThrowOnAttachmentModifications)
                    {
                        ThrowAttachmentsModificationsDetected();
                    }
                }
            }

            if (options.ThrowOnAttachmentModifications && modifiedAttachmentNames.Count != 0)
            {
                ThrowAttachmentsModificationsDetected();
            }
            return true;
        }


        private static bool ShouldResolveCountersConflict(BlittableJsonReaderObject currentMetadata, BlittableJsonReaderObject modifiedMetadata)
        {
            currentMetadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray currentCounters);
            modifiedMetadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray modifiedCounters);
            Debug.Assert(currentCounters != null || modifiedCounters != null, "Cannot happen. We verified that we have a conflict in @counters.");

            if (currentCounters == null)
                return true;

            return currentCounters.Length != modifiedCounters.Length ||
                   currentCounters.All(modifiedCounters.Contains) == false;
        }

        private static bool ShouldResolveTimeSeriesConflict(BlittableJsonReaderObject currentMetadata, BlittableJsonReaderObject modifiedMetadata)
        {
            currentMetadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray currentTimeSeries);
            modifiedMetadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray modifiedTimeSeries);
            Debug.Assert(currentTimeSeries != null || modifiedTimeSeries != null, "Cannot happen. We verified that we have a conflict in @timeseries.");

            if (currentTimeSeries == null)
                return true;

            return currentTimeSeries.Length != modifiedTimeSeries.Length ||
                   currentTimeSeries.All(modifiedTimeSeries.Contains) == false;
        }
    }

    [Flags]
    public enum DocumentCompareResult
    {
        None = 0,

        NotEqual = 0x1,
        Equal = 0x2,

        AttachmentsNotEqual = 0x4,

        CountersNotEqual = 0x8,

        TimeSeriesNotEqual = 0x10
    }
}

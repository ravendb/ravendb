using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server.Strings;

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

            if (ComparePropertiesExceptStartingWithAt(original, modified, false, options) == DocumentCompareResult.NotEqual)
                return DocumentCompareResult.NotEqual;

            return result;
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

            return ComparePropertiesExceptStartingWithAt(currentMetadata, objMetadata, true, options);
        }

        [DoesNotReturn]
        private static void ThrowAttachmentsModificationsDetected()
        {
            throw new InvalidOperationException("Illegal modifications of '@attachments' detected");
        }
        
        private static bool IsSignificantMetadataProperty(ReadOnlySpan<byte> property, in DocumentCompareOptions options)
        {
            if (Constants.Documents.Metadata.CollectionAsSpan.IsEqualConstant(property) ||
                Constants.Documents.Metadata.ExpiresAsSpan.IsEqualConstant(property) ||
                Constants.Documents.Metadata.RefreshAsSpan.IsEqualConstant(property))
            {
                return true;
            }

            // Archival properties are significant only when the option is enabled
            if (options.CompareDataArchivalMetadata &&
                (Constants.Documents.Metadata.ArchiveAtAsSpan.IsEqualConstant(property) ||
                 Constants.Documents.Metadata.ArchivedAsSpan.IsEqualConstant(property)))
            {
                return true;
            }

            return false;
        }

        private static DocumentCompareResult ComparePropertiesExceptStartingWithAt(
            BlittableJsonReaderObject current,
            BlittableJsonReaderObject modified,
            bool isMetadata,
            in DocumentCompareOptions options)
        {
            var resolvedAttachmentConflict = false;
            var resolvedCountersConflict = false;
            var resolvedTimeSeriesConflict = false;

            // Two passes over the raw property tables instead of materializing every name into a
            // HashSet<string>: pass 0 walks `current` and compares values against `modified`, pass 1
            // walks `modified` only to catch properties that exist there alone - anything found in
            // `current` was already handled. Names stay as UTF-8 spans and flat values compare raw.
            for (var pass = 0; pass < 2; pass++)
            {
                var doc = pass == 0 ? current : modified;
                var other = pass == 0 ? modified : current;
                var count = doc.Count;

                for (var i = 0; i < count; i++)
                {
                    var property = doc.GetPropertyNameByIndexAsSpan(i);
                    var indexInOther = other.GetPropertyIndex(property);

                    if (pass == 1 && indexInOther != -1)
                        continue; // both sides have it - pass 0 already dealt with it

                    var existsInBoth = indexInOther != -1;

                    if (property.Length > 0 && property[0] == (byte)'@')
                    {
                        if (isMetadata)
                        {
                            if (Constants.Documents.Metadata.AttachmentsAsSpan.IsEqualConstant(property))
                            {
                                if (options.TryMergeMetadataConflicts)
                                {
                                    if (existsInBoth == false)
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
                            else if (Constants.Documents.Metadata.CountersAsSpan.IsEqualConstant(property))
                            {
                                if (options.TryMergeMetadataConflicts)
                                {
                                    if (existsInBoth == false)
                                    {
                                        // Resolve when just 1 document have counters
                                        resolvedCountersConflict = true;
                                        continue;
                                    }

                                    resolvedCountersConflict = ShouldResolveCountersConflict(current, modified);
                                    continue;
                                }
                            }
                            else if (Constants.Documents.Metadata.TimeSeriesAsSpan.IsEqualConstant(property))
                            {
                                if (options.TryMergeMetadataConflicts)
                                {
                                    if (existsInBoth == false)
                                    {
                                        // Resolve when just 1 document have time-series
                                        resolvedTimeSeriesConflict = true;
                                        continue;
                                    }

                                    resolvedTimeSeriesConflict = ShouldResolveTimeSeriesConflict(current, modified);
                                    continue;
                                }
                            }
                            else if (IsSignificantMetadataProperty(property, options) == false)
                                continue;
                        }
                        else if (Constants.Documents.Metadata.KeyAsSpan.IsEqualConstant(property))
                        {
                            continue;
                        }
                    }

                    if (existsInBoth == false)
                        return DocumentCompareResult.NotEqual;

                    if (pass == 1)
                        continue; // unreachable (pass 1 only sees one-sided properties), kept for clarity

                    if (BlittableJsonReaderObject.TryCompareValuesByIndex(doc, i, other, indexInOther, out var equal) == false)
                        equal = Equals(doc.GetValueByIndex(i), other.GetValueByIndex(indexInOther));

                    if (equal == false)
                        return DocumentCompareResult.NotEqual;
                }
            }
        
            var shouldRecreateAttachment = resolvedAttachmentConflict ? DocumentCompareResult.AttachmentsNotEqual : DocumentCompareResult.None;
            var shouldRecreateCounters = resolvedCountersConflict ? DocumentCompareResult.CountersNotEqual : DocumentCompareResult.None;
            var shouldRecreateTimeSeries = resolvedTimeSeriesConflict ? DocumentCompareResult.TimeSeriesNotEqual : DocumentCompareResult.None;

            return DocumentCompareResult.Equal | shouldRecreateAttachment | shouldRecreateCounters | shouldRecreateTimeSeries;
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
                    if (ComparePropertiesExceptStartingWithAt(attachment.Value, modifiedAttachment, false, options) == DocumentCompareResult.NotEqual)
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

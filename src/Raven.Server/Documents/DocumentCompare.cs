using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Sparrow.Json;

namespace Raven.Server.Documents
{
    public static class DocumentCompare
    {
        public static DocumentCompareResult IsEqualTo(BlittableJsonReaderObject original, BlittableJsonReaderObject modified,
            bool tryMergeAttachmentsConflict)
        {
            if (ReferenceEquals(original, modified))
                return DocumentCompareResult.Equal;

            if (original == null || modified == null)
                return DocumentCompareResult.NotEqual;

            BlittableJsonReaderObject.AssertNoModifications(original, nameof(original), true);
            BlittableJsonReaderObject.AssertNoModifications(modified, nameof(modified), true);

            // Performance improvemnt: We compare the metadata first 
            // because that most of the time the metadata itself won't be the equal, so no need to compare all values

            var result = IsMetadataEqualTo(original, modified, tryMergeAttachmentsConflict);
            if (result == DocumentCompareResult.NotEqual)
                return DocumentCompareResult.NotEqual;

            if (ComparePropertiesExceptStartingWithAt(original, modified) == DocumentCompareResult.NotEqual)
                return DocumentCompareResult.NotEqual;

            return result;
        }

        private static DocumentCompareResult IsMetadataEqualTo(BlittableJsonReaderObject current, BlittableJsonReaderObject modified, bool tryMergeAttachmentsConflict)
        {
            if (modified == null)
                return DocumentCompareResult.NotEqual;

            BlittableJsonReaderObject currentMetadata;
            BlittableJsonReaderObject objMetadata;
            current.TryGet(Constants.Documents.Metadata.Key, out currentMetadata);
            modified.TryGet(Constants.Documents.Metadata.Key, out objMetadata);

            if (currentMetadata == null && objMetadata == null)
                return DocumentCompareResult.Equal;

            if (currentMetadata == null || objMetadata == null)
            {
                if (tryMergeAttachmentsConflict)
                {
                    if (currentMetadata == null)
                        currentMetadata = objMetadata;

                    // If the conflict is just on @metadata with @attachment we know how to resolve it.
                    if (currentMetadata.Count == 1 && currentMetadata.GetPropertyNames()[0].Equals(Constants.Documents.Metadata.Attachments, StringComparison.OrdinalIgnoreCase))
                        return DocumentCompareResult.Equal | DocumentCompareResult.ShouldRecreateDocument;
                }

                return DocumentCompareResult.NotEqual;
            }

            return ComparePropertiesExceptStartingWithAt(currentMetadata, objMetadata, true, tryMergeAttachmentsConflict);
        }

        private static DocumentCompareResult ComparePropertiesExceptStartingWithAt(BlittableJsonReaderObject current, BlittableJsonReaderObject modified, 
            bool isMetadata = false, bool tryMergeAttachmentsConflict = false)
        {
            var resolvedAttachmetConflict = false;

            var properties = new HashSet<string>(current.GetPropertyNames());
            foreach (var propertyName in modified.GetPropertyNames())
            {
                properties.Add(propertyName);
            }

            foreach (var property in properties)
            {
                if (property[0] == '@')
                {
                    if (isMetadata)
                    {
                        if (property.Equals(Constants.Documents.Metadata.Attachments, StringComparison.OrdinalIgnoreCase))
                        {
                            if (tryMergeAttachmentsConflict)
                            {
                                if (current.TryGetMember(property, out object currentAttachments) == false ||
                                    modified.TryGetMember(property, out object modifiedAttachments) == false)
                                {
                                    // Resolve when just 1 document have attachments
                                    resolvedAttachmetConflict = true;
                                    continue;
                                }

                                resolvedAttachmetConflict = ShouldResolveAttachmentsConflict(current, modified);
                                if (resolvedAttachmetConflict)
                                    continue;

                                return DocumentCompareResult.NotEqual;
                            }
                        }
                        else if (property.Equals(Constants.Documents.Metadata.Collection, StringComparison.OrdinalIgnoreCase) == false)
                            continue;
                    }
                    else if (property.Equals(Constants.Documents.Metadata.Key, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }
                }

                if (current.TryGetMember(property, out object currentProperty) == false ||
                    modified.TryGetMember(property, out object modifiedPropery) == false)
                {
                    return DocumentCompareResult.NotEqual;
                }

                if (Equals(currentProperty, modifiedPropery) == false)
                {
                    return DocumentCompareResult.NotEqual;
                }
            }

            return DocumentCompareResult.Equal | (resolvedAttachmetConflict ? DocumentCompareResult.ShouldRecreateDocument : DocumentCompareResult.None);
        }

        private static bool ShouldResolveAttachmentsConflict(BlittableJsonReaderObject currentMetadata, BlittableJsonReaderObject modifiedMetadata)
        {
            currentMetadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray currentAttachments);
            modifiedMetadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray modifiedAttachments);
            Debug.Assert(currentAttachments != null || modifiedAttachments != null, "Cannot happen. We verified that we have a conflict in @attachments.");

            var currentAttachmentNames = new Dictionary<string, BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase);
            if (currentAttachments != null)
            {
                foreach (BlittableJsonReaderObject attachment in currentAttachments)
                {
                    if (attachment.TryGet(nameof(AttachmentResult.Name), out string name) == false)
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
                    if (attachment.TryGet(nameof(AttachmentResult.Name), out string name) == false)
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
                    if (ComparePropertiesExceptStartingWithAt(attachment.Value, modifiedAttachment) == DocumentCompareResult.NotEqual)
                        return false;

                    modifiedAttachmentNames.Remove(attachment.Key);
                }
            }

            return true;
        }
    }

    [Flags]
    public enum DocumentCompareResult
    {
        None = 0,

        NotEqual = 0x1,
        Equal = 0x2,

        ShouldRecreateDocument = 0x8,
    }
}
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
        public static DocumentCompareResult IsEqualTo(BlittableJsonReaderObject currentDocument, BlittableJsonReaderObject targetDocument,
            bool tryMergeAttachmentsConflict)
        {
            // Performance improvemnt: We compare the metadata first 
            // because that most of the time the metadata itself won't be the equal, so no need to compare all values

            var result = IsMetadataEqualTo(currentDocument, targetDocument, tryMergeAttachmentsConflict);
            if (result == DocumentCompareResult.NotEqual)
                return DocumentCompareResult.NotEqual;

            if (ComparePropertiesExceptStartingWithAt(currentDocument, targetDocument) == DocumentCompareResult.NotEqual)
                return DocumentCompareResult.NotEqual;

            return result;
        }

        private static DocumentCompareResult IsMetadataEqualTo(BlittableJsonReaderObject currentDocument, BlittableJsonReaderObject targetDocument, bool tryMergeAttachmentsConflict)
        {
            if (targetDocument == null)
                return DocumentCompareResult.NotEqual;

            BlittableJsonReaderObject myMetadata;
            BlittableJsonReaderObject objMetadata;
            currentDocument.TryGet(Constants.Documents.Metadata.Key, out myMetadata);
            targetDocument.TryGet(Constants.Documents.Metadata.Key, out objMetadata);

            if (myMetadata == null && objMetadata == null)
                return DocumentCompareResult.Equal;

            if (myMetadata == null || objMetadata == null)
            {
                if (tryMergeAttachmentsConflict)
                {
                    if (myMetadata == null)
                        myMetadata = objMetadata;

                    // If the conflict is just on @metadata with @attachment we know how to resolve it.
                    if (myMetadata.Count == 1 && myMetadata.GetPropertyNames()[0].Equals(Constants.Documents.Metadata.Attachments, StringComparison.OrdinalIgnoreCase))
                        return DocumentCompareResult.Equal | DocumentCompareResult.ShouldRecreateDocument;
                }

                return DocumentCompareResult.NotEqual;
            }

            return ComparePropertiesExceptStartingWithAt(myMetadata, objMetadata, true, tryMergeAttachmentsConflict);
        }

        private static DocumentCompareResult ComparePropertiesExceptStartingWithAt(BlittableJsonReaderObject myObject, BlittableJsonReaderObject otherObject, 
            bool isMetadata = false, bool tryMergeAttachmentsConflict = false)
        {
            var resolvedAttachmetConflict = false;

            var properties = new HashSet<string>(myObject.GetPropertyNames());
            foreach (var propertyName in otherObject.GetPropertyNames())
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
                                if (myObject.TryGetMember(property, out object myAttachments) == false ||
                                    otherObject.TryGetMember(property, out object otherAttachments) == false)
                                {
                                    // Resolve when just 1 document have attachments
                                    resolvedAttachmetConflict = true;
                                    continue;
                                }

                                resolvedAttachmetConflict = ShouldResolveAttachmentsConflict(myObject, otherObject);
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

                if (myObject.TryGetMember(property, out object myProperty) == false ||
                    otherObject.TryGetMember(property, out object otherPropery) == false)
                {
                    return DocumentCompareResult.NotEqual;
                }

                if (Equals(myProperty, otherPropery) == false)
                {
                    return DocumentCompareResult.NotEqual;
                }
            }

            return DocumentCompareResult.Equal | (resolvedAttachmetConflict ? DocumentCompareResult.ShouldRecreateDocument : DocumentCompareResult.None);
        }

        private static bool ShouldResolveAttachmentsConflict(BlittableJsonReaderObject myMetadata, BlittableJsonReaderObject otherMetadata)
        {
            myMetadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray myAttachments);
            otherMetadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray otherAttachments);
            Debug.Assert(myAttachments != null || otherAttachments != null, "Cannot happen. We verified that we have a conflict in @attachments.");

            var myAttachmentNames = new Dictionary<string, BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase);
            if (myAttachments != null)
            {
                foreach (BlittableJsonReaderObject attachment in myAttachments)
                {
                    if (attachment.TryGet(nameof(AttachmentResult.Name), out string name) == false)
                        return false;   // Attachment must have a name. The user modified the value?

                    if (myAttachmentNames.ContainsKey(name))
                        // The node itself has a conflict
                        return false;
                    myAttachmentNames.Add(name, attachment);
                }
            }

            var otherAttachmentNames = new Dictionary<string, BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase);
            if (otherAttachments != null)
            {
                foreach (BlittableJsonReaderObject attachment in otherAttachments)
                {
                    if (attachment.TryGet(nameof(AttachmentResult.Name), out string name) == false)
                        return false;   // Attachment must have a name. The user modified the value?

                    if (otherAttachmentNames.ContainsKey(name))
                        // The node itself has a conflict
                        return false;
                    otherAttachmentNames.Add(name, attachment);
                }
            }

            foreach (var attachment in myAttachmentNames)
            {
                if (otherAttachmentNames.TryGetValue(attachment.Key, out var otherAttachment))
                {
                    if (ComparePropertiesExceptStartingWithAt(attachment.Value, otherAttachment) == DocumentCompareResult.NotEqual)
                        return false;

                    otherAttachmentNames.Remove(attachment.Key);
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
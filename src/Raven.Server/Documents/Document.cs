using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Client;
using Raven.Client.Documents.Replication.Messages;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class Document
    {
        public static readonly Document ExplicitNull = new Document();

        private ulong? _hash;
        private bool _metadataEnsured;

        public long Etag;
        public LazyStringValue Key;
        public LazyStringValue LoweredKey;
        public long StorageId;
        public BlittableJsonReaderObject Data;
        public float? IndexScore;
        public ChangeVectorEntry[] ChangeVector;
        public DateTime LastModified;
        public DocumentFlags Flags;
        public NonPersistentDocumentFlags NonPersistentFlags;
        public short TransactionMarker;

        public unsafe ulong DataHash
        {
            get
            {
                if (_hash.HasValue == false)
                    _hash = Hashing.XXHash64.Calculate(Data.BasePointer, (ulong)Data.Size);

                return _hash.Value;
            }
        }

        public void EnsureMetadata(float? indexScore = null)
        {
            if (_metadataEnsured)
                return;

            _metadataEnsured = true;

            DynamicJsonValue mutatedMetadata;
            BlittableJsonReaderObject metadata;
            if (Data.TryGet(Constants.Documents.Metadata.Key, out metadata))
            {
                if (metadata.Modifications == null)
                    metadata.Modifications = new DynamicJsonValue(metadata);

                mutatedMetadata = metadata.Modifications;
            }
            else
            {
                Data.Modifications = new DynamicJsonValue(Data)
                {
                    [Constants.Documents.Metadata.Key] = mutatedMetadata = new DynamicJsonValue()
                };
            }

            mutatedMetadata[Constants.Documents.Metadata.Etag] = Etag;
            mutatedMetadata[Constants.Documents.Metadata.Id] = Key;
            //mutatedMetadata[Constants.Documents.Metadata.ChangeVector] = ChangeVector;
            if (indexScore.HasValue)
                mutatedMetadata[Constants.Documents.Metadata.IndexScore] = indexScore;

            _hash = null;
        }

        public void RemoveAllPropertiesExceptMetadata()
        {
            foreach (var property in Data.GetPropertyNames())
            {
                if (string.Equals(property, Constants.Documents.Metadata.Key, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (Data.Modifications == null)
                    Data.Modifications = new DynamicJsonValue(Data);

                Data.Modifications.Remove(property);
            }

            _hash = null;
        }

        public bool Expired(DateTime currentDate)
        {
            string expirationDate;
            BlittableJsonReaderObject metadata;
            if (Data.TryGet(Constants.Documents.Metadata.Key, out metadata) &&
                metadata.TryGet(Constants.Documents.Expiration.ExpirationDate, out expirationDate))
            {
                var expirationDateTime = DateTime.ParseExact(expirationDate, new[] { "o", "r" }, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                if (expirationDateTime < currentDate)
                    return true;
            }
            return false;
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

        public static DocumentCompareResult ComparePropertiesExceptStartingWithAt(BlittableJsonReaderObject myObject, BlittableJsonReaderObject otherObject, 
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

                                resolvedAttachmetConflict = AttachmentsStorage.ShouldResolveAttachmentsConflict(myObject, otherObject);
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
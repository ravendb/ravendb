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

        private static bool IsMetadataEqualTo(BlittableJsonReaderObject currentDocument, BlittableJsonReaderObject targetDocument)
        {
            if (targetDocument == null)
                return false;

            BlittableJsonReaderObject myMetadata;
            BlittableJsonReaderObject objMetadata;
            currentDocument.TryGet(Constants.Documents.Metadata.Key, out myMetadata);
            targetDocument.TryGet(Constants.Documents.Metadata.Key, out objMetadata);

            if (myMetadata == null && objMetadata == null)
                return true;

            if (myMetadata == null || objMetadata == null)
                return false;

            return ComparePropertiesExceptionStartingWithAt(myMetadata, objMetadata, isMetadata: true);
        }

        public static bool IsEqualTo(BlittableJsonReaderObject currentDocument, BlittableJsonReaderObject targetDocument)
        {
            // Performance improvemnt: We compare the metadata first 
            // because that most of the time the metadata itself won't be the equal, so no need to compare all values

            return IsMetadataEqualTo(currentDocument, targetDocument) &&
                   ComparePropertiesExceptionStartingWithAt(currentDocument, targetDocument);
        }

        private static bool ComparePropertiesExceptionStartingWithAt(BlittableJsonReaderObject myObject,
            BlittableJsonReaderObject otherObject, bool isMetadata = false)
        {
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
                        if (property.Equals(Constants.Documents.Metadata.Collection, StringComparison.OrdinalIgnoreCase) == false &&
                            property.Equals(Constants.Documents.Metadata.Attachments, StringComparison.OrdinalIgnoreCase) == false)
                            continue;
                    }
                    else if (property.Equals(Constants.Documents.Metadata.Key, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }
                }

                object myProperty;
                object otherPropery;

                if (myObject.TryGetMember(property, out myProperty) == false)
                    return false;

                if (otherObject.TryGetMember(property, out otherPropery) == false)
                    return false;

                if (Equals(myProperty, otherPropery) == false)
                    return false;
            }

            return true;
        }
    }
}
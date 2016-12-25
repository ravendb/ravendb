using System;
using System.Globalization;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Replication.Messages;
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
            if (Data.TryGet(Constants.Metadata.Key, out metadata))
            {
                if (metadata.Modifications == null)
                    metadata.Modifications = new DynamicJsonValue(metadata);

                mutatedMetadata = metadata.Modifications;
            }
            else
            {
                Data.Modifications = new DynamicJsonValue(Data)
                {
                    [Constants.Metadata.Key] = mutatedMetadata = new DynamicJsonValue()
                };
            }

            mutatedMetadata[Constants.Metadata.Etag] = Etag;
            mutatedMetadata[Constants.Metadata.Id] = Key;

            if (indexScore.HasValue)
                mutatedMetadata[Constants.Metadata.IndexScore] = indexScore;

            _hash = null;
        }

        public void RemoveAllPropertiesExceptMetadata()
        {
            foreach (var property in Data.GetPropertyNames())
            {
                if (string.Equals(property, Constants.Metadata.Key, StringComparison.OrdinalIgnoreCase))
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
            if (Data.TryGet(Constants.Metadata.Key, out metadata) &&
                metadata.TryGet(Constants.Expiration.RavenExpirationDate, out expirationDate))
            {
                var expirationDateTime = DateTime.ParseExact(expirationDate, new[] { "o", "r" }, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                if (expirationDateTime < currentDate)
                    return true;
            }
            return false;
        }

        public bool CompareMetadata(BlittableJsonReaderObject obj, string[] excludedShallowProperties)
        {
            BlittableJsonReaderObject myMetadata;
            BlittableJsonReaderObject objMetadata;
            if (Data.TryGet(Constants.Metadata.Key, out myMetadata) && obj.TryGet(Constants.Metadata.Key, out objMetadata))
            {
                foreach (var property in myMetadata.GetPropertyNames().Union(objMetadata.GetPropertyNames()))
                {
                    if (Array.IndexOf(excludedShallowProperties, property) >= 0)
                    {
                        continue;
                    }
                    
                    object myProperty = null;
                    object objProperty = null;
                    
                    if ((myMetadata.TryGetMember(property, out myProperty) | objMetadata.TryGetMember(property, out objProperty)) == false)
                    {
                        continue;
                    }

                    if (myProperty == null)
                    {
                        return false;
                    }

                    if (myProperty.Equals(objProperty))
                    {
                        continue;
                    }

                    return false;
                }
            }
            return true;
        }

        public bool CompareContent(BlittableJsonReaderObject obj, string[] excludedShallowProperties = null)
        {
            if (excludedShallowProperties == null)
            {
                excludedShallowProperties = new string[] { Constants.Metadata.Key };
            }

            foreach (var property in Data.GetPropertyNames())
            {
                if (Array.IndexOf(excludedShallowProperties, property) >= 0
                    || Data[property].Equals(obj[property]))
                {
                    continue;
                }
                return false;
            }
            return true;
        }

    }
}
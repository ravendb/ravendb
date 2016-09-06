using System;
using Raven.Abstractions.Data;
using Raven.Client.Replication.Messages;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents
{
    public class Document
    {
        public static readonly Document ExplicitNull = new Document();

        public const string NoCollectionSpecified = "Raven/Empty";
        public const string SystemDocumentsCollection = "Raven/SystemDocs";

        private ulong? _hash;
        private bool _metadataEnsured;

        public long Etag;
        public LazyStringValue Key;
        public LazyStringValue LoweredKey;
        public long StorageId;
        public BlittableJsonReaderObject Data;
        public ChangeVectorEntry[] ChangeVector;

        public Document()
        {
        }

        public unsafe ulong DataHash
        {
            get
            {
                if (_hash.HasValue == false)
                    _hash = Hashing.XXHash64.Calculate(Data.BasePointer, Data.Size);

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

        public static string GetCollectionName(Slice key, BlittableJsonReaderObject document, out bool isSystemDocument)
        {
            if (key.Size >= 6)
            {
                if ((key[0] == (byte)'R' || key[0] == (byte)'r') &&
                    (key[1] == (byte)'A' || key[1] == (byte)'a') &&
                    (key[2] == (byte)'V' || key[2] == (byte)'v') &&
                    (key[3] == (byte)'E' || key[3] == (byte)'e') &&
                    (key[4] == (byte)'N' || key[4] == (byte)'n') &&
                    (key[5] == (byte)'/'))
                {
                    isSystemDocument = true;
                    return SystemDocumentsCollection;
                }
            }

            return GetCollectionName(document, out isSystemDocument);
        }

        public static string GetCollectionName(string key, BlittableJsonReaderObject document, out bool isSystemDocument)
        {
            if (key != null && key.StartsWith("Raven/", StringComparison.OrdinalIgnoreCase))
            {
                isSystemDocument = true;
                return SystemDocumentsCollection;
            }

            return GetCollectionName(document, out isSystemDocument);
        }

        public static string GetCollectionName(BlittableJsonReaderObject document, out bool isSystemDocument)
        {
            isSystemDocument = false;
            string collectionName;
            BlittableJsonReaderObject metadata;
            if (document.TryGet(Constants.Metadata.Key, out metadata) == false ||
                metadata.TryGet(Constants.Headers.RavenEntityName, out collectionName) == false)
            {
                collectionName = NoCollectionSpecified;
            }
            return collectionName;
        }
    }
}
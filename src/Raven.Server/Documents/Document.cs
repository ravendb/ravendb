using System;
using System.Diagnostics;
using Raven.Client;
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
        public LazyStringValue Id;
        public LazyStringValue LowerId;
        public long StorageId;
        public BlittableJsonReaderObject Data;
        public float? IndexScore;
        public string ChangeVector;
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

        public bool TryGetMetadata(out BlittableJsonReaderObject metadata) => 
            Data.TryGet(Constants.Documents.Metadata.Key, out metadata);

        public void EnsureMetadata()
        {
            if (_metadataEnsured)
                return;

            _metadataEnsured = true;
            DynamicJsonValue mutatedMetadata = null;
            if (Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
            {
                if (metadata.Modifications == null)
                    metadata.Modifications = new DynamicJsonValue(metadata);

                mutatedMetadata = metadata.Modifications;
            }

            Data.Modifications = new DynamicJsonValue(Data)
            {
                [Constants.Documents.Metadata.Key] = (object)metadata ?? (mutatedMetadata = new DynamicJsonValue())
            };

            mutatedMetadata[Constants.Documents.Metadata.Id] = Id;
            if (ChangeVector != null)
                mutatedMetadata[Constants.Documents.Metadata.ChangeVector] = ChangeVector;        
            if (Flags != DocumentFlags.None)
                mutatedMetadata[Constants.Documents.Metadata.Flags] = Flags.ToString();
            Debug.Assert(LastModified != DateTime.MinValue, $"LastModified cannot be DateTime.MinValue. {Id}");
            mutatedMetadata[Constants.Documents.Metadata.LastModified] = LastModified;
            if (IndexScore.HasValue)
                mutatedMetadata[Constants.Documents.Metadata.IndexScore] = IndexScore;

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

        public void ResetModifications()
        {
            _metadataEnsured = false;
            Data.Modifications = null;
        }
    }
}

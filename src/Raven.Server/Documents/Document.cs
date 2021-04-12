using System;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class Document : IDisposable
    {
        public static readonly Document ExplicitNull = new Document();

        private bool _disposed;
        private ulong? _hash;
        private bool _metadataEnsured;

        public long Etag;
        public LazyStringValue Id;
        public LazyStringValue LowerId;
        public long StorageId;
        public BlittableJsonReaderObject Data;
        public float? IndexScore;
        public SpatialResult? Distance;
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
            if (LastModified != DateTime.MinValue)
                mutatedMetadata[Constants.Documents.Metadata.LastModified] = LastModified;
            if (IndexScore.HasValue)
                mutatedMetadata[Constants.Documents.Metadata.IndexScore] = IndexScore;
            if (Distance.HasValue)
            {
                mutatedMetadata[Constants.Documents.Metadata.SpatialResult] = Distance.Value.ToJson();
            }

            _hash = null;
        }

        public void ResetModifications()
        {
            _metadataEnsured = false;
            Data.Modifications = null;
        }

        public Document Clone(DocumentsOperationContext context)
        {
            return new Document
            {
                Etag = Etag,
                StorageId = StorageId,
                IndexScore = IndexScore,
                Distance = Distance,
                ChangeVector = ChangeVector,
                LastModified = LastModified,
                Flags = Flags,
                NonPersistentFlags = NonPersistentFlags,
                TransactionMarker = TransactionMarker,

                Id = context.GetLazyString(Id),
                LowerId = context.GetLazyString(LowerId),
                Data = Data.Clone(context),
            };
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            Id?.Dispose();
            Id = null;

            LowerId?.Dispose();
            LowerId = null;

            Data?.Dispose();
            Data = null;

            _disposed = true;
        }
    }

    [Flags]
    public enum DocumentFields
    {
        None = 0,
        Id = 1 << 0,
        LowerId = 1 << 1,
        Etag = 1 << 2,
        StorageId = 1 << 3,
        Data = 1 << 4,
        ChangeVector = 1 << 5,
        LastModified = 1 << 6,
        Flags = 1 << 7,
        TransactionMarker = 1 << 8,

        All = Id | LowerId | Etag | StorageId | Data | ChangeVector | LastModified | Flags | TransactionMarker
    }


    public struct SpatialResult
    {
        public double Distance, Latitude, Longitude;

        public static SpatialResult Invalid = new SpatialResult
        {
            Distance = double.NaN,
            Latitude = double.NaN,
            Longitude = double.NaN
        };

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Distance)] = Distance,
                [nameof(Latitude)] = Latitude,
                [nameof(Longitude)] = Longitude,
            };
        }
    }
}

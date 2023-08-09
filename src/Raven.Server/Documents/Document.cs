using System;
using System.Collections.Generic;
using Raven.Client;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public sealed class TimeSeriesStream
    {
        public IEnumerable<DynamicJsonValue> TimeSeries;
        public string Key;
    }

    public class Document : IDisposable
    {
        public static readonly Document ExplicitNull = new Document();

        public LazyStringValue Id;
        public LazyStringValue LowerId;
        public BlittableJsonReaderObject Data;
        public string ChangeVector;
        public TimeSeriesStream TimeSeriesStream;
        public SpatialResult? Distance;

        private ulong? _hash;
        public long Etag;
        public long StorageId;
        public float? IndexScore;
        public DateTime LastModified;
        public DocumentFlags Flags;
        public NonPersistentDocumentFlags NonPersistentFlags;
        public short TransactionMarker;
        public bool IgnoreDispose;

        private bool _metadataEnsured;
        private bool _disposed;

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

        
        
        public void EnsureDocumentId()
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
            _hash = null;
        }
        
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

        public Document Clone(JsonOperationContext context)
        {
            var newData = Data.Clone(context);
            return CloneWith(context, newData);
        }

        public Document CloneWith(JsonOperationContext context, BlittableJsonReaderObject newData)
        {
            var newId = context.GetLazyString(Id);
            var newLowerId = context.GetLazyString(LowerId);
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
                Id = newId,
                LowerId = newLowerId,
                Data = newData,
            };
        }

        public void Dispose()
        {
            if (_disposed || IgnoreDispose)
                return;

            Id?.Dispose();
            Id = null;

            LowerId?.Dispose();
            LowerId = null;

            Data?.Dispose();
            Data = null;

            _disposed = true;
        }

        public override string ToString()
        {
            return $"id:{Id}, cv:{ChangeVector}";
        }
    }

    [Flags]
    public enum DocumentFields
    {
        Default = 0,
        Id = 1 << 0,
        LowerId = 1 << 1,
        Data = 1 << 4,
        ChangeVector = 1 << 5,

        All = Id | LowerId | Data | ChangeVector
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

        public static explicit operator SpatialResult?(Corax.Utils.Spatial.SpatialResult? coraxSpatialResult)
        {
            if (coraxSpatialResult is null)
                return null;
            
            return new SpatialResult {Distance = coraxSpatialResult.Value.Distance, Latitude = coraxSpatialResult.Value.Latitude, Longitude = coraxSpatialResult.Value.Longitude};
        }

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

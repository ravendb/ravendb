using System;
using System.Collections.Generic;
using Raven.Client;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class TimeSeriesStream
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

        private bool _metadataEnsured;
        public bool IgnoreDispose;
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
            return Id;
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

﻿using System;
using System.Diagnostics;
using System.Globalization;
using Raven.Client;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.ServerWide.Context;
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
        public LazyStringValue Collection;
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

        public void EnsureMetadata(DocumentsOperationContext context = null)
        {
            if (_metadataEnsured)
                return;

            _metadataEnsured = true;
            DynamicJsonValue mutatedMetadata;

            if (Data == null)
            {
                Debug.Assert(context != null);
                Data = context.ReadObject(new DynamicJsonValue(), "Zombies");
                Data.Modifications = new DynamicJsonValue(Data)
                {
                    [Constants.Documents.Metadata.Key] = mutatedMetadata = new DynamicJsonValue()
                };
            }
            else if (Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
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
            mutatedMetadata[Constants.Documents.Metadata.Id] = Id;
            if (ChangeVector != null)
                mutatedMetadata[Constants.Documents.Metadata.ChangeVector] = ChangeVector.ToJson();
            if (Flags != DocumentFlags.None)
                mutatedMetadata[Constants.Documents.Metadata.Flags] = Flags.ToString();
            if (IndexScore.HasValue)
                mutatedMetadata[Constants.Documents.Metadata.IndexScore] = IndexScore;
            if (Collection != null) // Currently in use only for delete revision. See RavenDB-7420.
                mutatedMetadata[Constants.Documents.Metadata.Collection] = Collection;

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
                var expirationDateTime = DateTime.ParseExact(expirationDate, new[] {"o", "r"}, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                if (expirationDateTime < currentDate)
                    return true;
            }
            return false;
        }
    }
}
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Raven.Client;
using Raven.Server.Documents.Indexes.Static;
using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents
{
    public class CollectionNameComparer : IEqualityComparer<CollectionName>
    {
        public static readonly CollectionNameComparer Instance = new CollectionNameComparer();


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(CollectionName x, CollectionName y)
        {
            if (x == y) return true;
            if (x == null || y == null) return false;
            return string.Equals(x.Name, y.Name, StringComparison.OrdinalIgnoreCase);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(CollectionName obj)
        {
            return obj.Name != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Name) : 0;
        }
    }

    public class CollectionName
    {
        public const string EmptyCollection = "@empty";
        public const string SystemCollection = "@system";

        public static readonly StringSegment EmptyCollectionSegment;
        public static readonly StringSegment MetadataKeySegment;
        public static readonly StringSegment MetadataCollectionSegment;

        private readonly string _documents;
        private readonly string _tombstones;

        public readonly string Name;
        public readonly bool IsSystem;

        static CollectionName()
        {
            EmptyCollectionSegment = new StringSegment(EmptyCollection);
            MetadataKeySegment = new StringSegment(Constants.Documents.Metadata.Key);
            MetadataCollectionSegment = new StringSegment(Constants.Documents.Metadata.Collection);
        }

        public CollectionName(string name)
        {
            Name = name;
            IsSystem = IsSystemCollection(name);

            _documents = GetName(CollectionTableType.Documents);
            _tombstones = GetName(CollectionTableType.Tombstones);
        }

        public string GetTableName(CollectionTableType type)
        {
            switch (type)
            {
                case CollectionTableType.Documents:
                    return _documents;
                case CollectionTableType.Tombstones:
                    return _tombstones;
                default:
                    throw new NotSupportedException($"Collection table type '{type}' is not supported.");
            }
        }

        public override string ToString()
        {
            return $"Collection: '{Name}'";
        }

        protected bool Equals(CollectionName other)
        {
            return string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((CollectionName)obj);
        }

        public override int GetHashCode()
        {
            return Name != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(Name) : 0;
        }

        public static bool operator ==(CollectionName left, CollectionName right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(CollectionName left, CollectionName right)
        {
            return Equals(left, right) == false;
        }

        public static bool IsSystemCollection(string collection)
        {
            return string.Equals(collection, SystemCollection, StringComparison.OrdinalIgnoreCase);
        }

        public static unsafe string GetCollectionName(Slice id, BlittableJsonReaderObject document)
        {
            if (IsSystemDocument(id.Content.Ptr, id.Size, out bool _))
            {
                return SystemCollection;
            }

            return GetCollectionName(document);
        }

        public static unsafe bool IsSystemDocument(byte* buffer, int length, out bool isHiLo)
        {
            isHiLo = false;

            if (length < 6)
                return false;

            // case insensitive 'Raven/' match without doing allocations

            if ((buffer[0] != (byte)'R' && buffer[0] != (byte)'r') ||
                (buffer[1] != (byte)'A' && buffer[1] != (byte)'a') ||
                (buffer[2] != (byte)'V' && buffer[2] != (byte)'v') ||
                (buffer[3] != (byte)'E' && buffer[3] != (byte)'e') ||
                (buffer[4] != (byte)'N' && buffer[4] != (byte)'n') ||
                buffer[5] != (byte)'/')
                return false;

            if (length < 11)
                return true;

            // Now need to find if the next bits are 'hilo/'
            if ((buffer[6] == (byte)'H' || buffer[6] == (byte)'h') &&
                (buffer[7] == (byte)'I' || buffer[7] == (byte)'i') &&
                (buffer[8] == (byte)'L' || buffer[8] == (byte)'l') &&
                (buffer[9] == (byte)'O' || buffer[9] == (byte)'o') &&
                buffer[10] == (byte)'/')
            {
                isHiLo = true;
            }

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsSystemCollectionName(string id)
        {
            if (id.Length < 6)
                return false;

            // case insensitive 'Raven/' match without doing allocations

            if ( id[5] != '/' ||
                (id[0] != 'R' && id[0] != 'r') ||
                (id[1] != 'A' && id[1] != 'a') ||
                (id[2] != 'V' && id[2] != 'v') ||
                (id[3] != 'E' && id[3] != 'e') ||
                (id[4] != 'N' && id[4] != 'n'))
                return false;

            return true;
        }
        
        public static string GetCollectionName(string id, BlittableJsonReaderObject document)
        {
            if (id != null && IsSystemCollectionName(id))
                return SystemCollection;

            return GetCollectionName(document);
        }

        public static string GetCollectionName(DynamicBlittableJson document)
        {
            dynamic dynamicDocument = document;
            string id = dynamicDocument.Id;

            if (id != null && IsSystemCollectionName(id))
                return SystemCollection;

            return GetCollectionName(document.BlittableJson);
        }

        public static LazyStringValue GetLazyCollectionNameFrom(JsonOperationContext context, BlittableJsonReaderObject document)
        {
            BlittableJsonReaderObject metadata;
            LazyStringValue collectionName;
            if (document.TryGet(MetadataKeySegment, out metadata) == false ||
                metadata.TryGet(MetadataCollectionSegment, out collectionName) == false)
            {
                return context.GetLazyStringForFieldWithCaching(EmptyCollectionSegment);
            }
            return collectionName;
        }

        public static string GetCollectionName(BlittableJsonReaderObject document)
        {
            string collectionName;
            BlittableJsonReaderObject metadata;

            if(document == null)
                return EmptyCollection;

            document.NoCache = true;

            if (document.TryGet(MetadataKeySegment, out metadata) == false || metadata.TryGet(MetadataCollectionSegment, out collectionName) == false)
            {
                collectionName = EmptyCollection;
            }

            return collectionName;
        }

        public static string GetTablePrefix(CollectionTableType type)
        {
            return $"Collection.{type}.";
        }

        private string GetName(CollectionTableType type)
        {
            return $"{GetTablePrefix(type)}{Name.ToLowerInvariant()}";
        }
    }

    public enum CollectionTableType
    {
        Documents,
        Tombstones
    }
}
﻿using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications
{
    public class DatabaseStatsChanged : Notification
    {
        private DatabaseStatsChanged() : base(NotificationType.DatabaseStatsChanged)
        {
        }

        public override string Id { get; } = string.Empty;

        public long CountOfDocuments { get; private set; }

        public long CountOfIndexes { get; private set; }

        public long CountOfStaleIndexes { get; private set; }

        public long AllDocumentsEtag { get; private set; }

        public List<ModifiedCollection> ModifiedCollections { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(CountOfDocuments)] = CountOfDocuments;
            json[nameof(CountOfIndexes)] = CountOfIndexes;
            json[nameof(CountOfStaleIndexes)] = CountOfStaleIndexes;
            json[nameof(AllDocumentsEtag)] = AllDocumentsEtag;
            json[nameof(ModifiedCollections)] = new DynamicJsonArray(ModifiedCollections.Select(x => x.ToJson()));

            return json;
        }

        public static DatabaseStatsChanged Create(long countOfDocs, int countOfIndexes, int countOfStaleIndexes, long allDocumentsEtag, List<ModifiedCollection> modifiedCollections)
        {
            return new DatabaseStatsChanged
            {
                IsPersistent = false,
                Title = null,
                Message = null,
                Severity = NotificationSeverity.Info,
                CountOfDocuments = countOfDocs,
                AllDocumentsEtag = ComputeEtag(allDocumentsEtag, countOfDocs),
                CountOfIndexes = countOfIndexes,
                CountOfStaleIndexes = countOfStaleIndexes,
                ModifiedCollections = modifiedCollections,
            };
        }

        private static unsafe long ComputeEtag(long etag, long numberOfDocuments)
        {
            var buffer = stackalloc long[2];
            buffer[0] = etag;
            buffer[1] = numberOfDocuments;
            return (long)Hashing.XXHash64.Calculate((byte*)buffer, sizeof(long) * 2);
        }

        public class ModifiedCollection
        {
            public readonly string Name;

            public readonly long Count;

            public readonly long LastDocumentEtag;

            public readonly long CollectionEtag;

            public ModifiedCollection(string name, long count, long lastDocumentEtag)
            {
                Name = name;
                Count = count;
                LastDocumentEtag = lastDocumentEtag;
                CollectionEtag = ComputeEtag(lastDocumentEtag, count);
            }

            public bool Equals(ModifiedCollection other)
            {
                if (ReferenceEquals(null, other))
                    return false;
                if (ReferenceEquals(this, other))
                    return true;

                return string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase) && Count == other.Count && LastDocumentEtag == other.LastDocumentEtag;
            }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue()
                {
                    [nameof(Name)] = Name,
                    [nameof(Count)] = Count,
                    [nameof(LastDocumentEtag)] = LastDocumentEtag,
                    [nameof(CollectionEtag)] = CollectionEtag
                };
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;

                var collection = obj as ModifiedCollection;
                if (collection == null)
                    return false;

                return Equals(collection);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = Name?.GetHashCode() ?? 0;
                    hashCode = (hashCode * 397) ^ Count.GetHashCode();
                    hashCode = (hashCode * 397) ^ LastDocumentEtag.GetHashCode();
                    return hashCode;
                }
            }
        }
    }
}
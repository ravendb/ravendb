﻿using System;
using System.Collections.Generic;
using System.Linq;
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

        public long CountOfConflicts { get; private set; }

        public long CountOfIndexes { get; private set; }

        public long CountOfStaleIndexes { get; private set; }

        public long CountOfIndexingErrors { get; private set; }

        public DateTime? LastIndexingErrorTime { get; private set; }

        public string GlobalChangeVector { get; private set; }

        public long LastEtag { get; private set; }

        public List<ModifiedCollection> ModifiedCollections { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(CountOfConflicts)] = CountOfConflicts;
            json[nameof(CountOfDocuments)] = CountOfDocuments;
            json[nameof(CountOfIndexes)] = CountOfIndexes;
            json[nameof(CountOfStaleIndexes)] = CountOfStaleIndexes;
            json[nameof(LastEtag)] = LastEtag;
            json[nameof(GlobalChangeVector)] = GlobalChangeVector;
            json[nameof(CountOfIndexingErrors)] = CountOfIndexingErrors;
            json[nameof(LastIndexingErrorTime)] = LastIndexingErrorTime;
            json[nameof(ModifiedCollections)] = new DynamicJsonArray(ModifiedCollections.Select(x => x.ToJson()));

            return json;
        }

        public static DatabaseStatsChanged Create(long countOfConflicts, long countOfDocs, int countOfIndexes, int countOfStaleIndexes, string globalChangeVector,
            long lastEtag, long countOfIndexingErrors, DateTime? lastIndexingErrorTime, List<ModifiedCollection> modifiedCollections)
        {
            return new DatabaseStatsChanged
            {
                IsPersistent = false,
                Title = null,
                Message = null,
                Severity = NotificationSeverity.Info,
                CountOfDocuments = countOfDocs,
                CountOfConflicts = countOfConflicts,
                LastEtag = lastEtag,
                GlobalChangeVector = globalChangeVector,
                CountOfIndexingErrors = countOfIndexingErrors,
                CountOfIndexes = countOfIndexes,
                CountOfStaleIndexes = countOfStaleIndexes,
                LastIndexingErrorTime = lastIndexingErrorTime,
                ModifiedCollections = modifiedCollections
            };
        }

        public class ModifiedCollection
        {
            public readonly string Name;

            public readonly long Count;

            public readonly string LastDocumentChangeVector;

            public ModifiedCollection(string name, long count, string lastDocumentChangeVector)
            {
                Name = name;
                Count = count;
                LastDocumentChangeVector = lastDocumentChangeVector;
            }

            public bool Equals(ModifiedCollection other)
            {
                if (ReferenceEquals(null, other))
                    return false;
                if (ReferenceEquals(this, other))
                    return true;

                return string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase) && Count == other.Count && LastDocumentChangeVector == other.LastDocumentChangeVector;
            }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Name)] = Name,
                    [nameof(Count)] = Count,
                    [nameof(LastDocumentChangeVector)] = LastDocumentChangeVector
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
                    hashCode = (hashCode * 397) ^ LastDocumentChangeVector.GetHashCode();
                    return hashCode;
                }
            }
        }
    }
}
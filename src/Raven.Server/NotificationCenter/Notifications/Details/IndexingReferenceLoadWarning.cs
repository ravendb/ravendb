using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Server.Documents.Indexes.Workers;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public sealed class IndexingReferenceLoadWarning : INotificationDetails
    {
        internal const int MaxNumberOfDetailsPerIndex = 10;

        public Dictionary<string, WarningDetails> Warnings { get; }


        public IndexingReferenceLoadWarning()
        {
            Warnings = new Dictionary<string, WarningDetails>(StringComparer.OrdinalIgnoreCase);
        }

        public void Update(string indexName, WarningDetails warning)
        {
            if (Warnings.TryGetValue(indexName, out var warningDetails) == false)
                Warnings[indexName] = warningDetails = new WarningDetails();

            foreach (var (_, reference) in warning.Top10LoadedReferences)
            {
                warningDetails.Add(reference);
            }

            warningDetails.LastWarningTime = warning.LastWarningTime;
        }

        public DynamicJsonValue ToJson()
        {
            var djv = new DynamicJsonValue();

            foreach (var key in Warnings.Keys)
            {
                var details = Warnings[key];
                if (details == null)
                    continue;

                var list = new DynamicJsonValue();

                foreach (var reference in details.Top10LoadedReferences)
                {
                    list[reference.Key] = new DynamicJsonValue
                    {
                        [nameof(LoadedReference.ReferenceId)] = reference.Value.ReferenceId,
                        [nameof(LoadedReference.NumberOfLoads)] = reference.Value.NumberOfLoads
                    };
                }

                djv[key] = new DynamicJsonValue
                {
                    [nameof(WarningDetails.Top10LoadedReferences)] = list,
                    [nameof(WarningDetails.LastWarningTime)] = details.LastWarningTime
                };
            }

            return new DynamicJsonValue(GetType())
            {
                [nameof(Warnings)] = djv
            };
        }

        public sealed class WarningDetails
        {
            private readonly SortedList<int, LoadedReference> _sortedTop10 = new(AllowDuplicatedNumberOfLoadsComparer.Instance);

            public Dictionary<string, LoadedReference> Top10LoadedReferences { get; set; } = new();

            public DateTime LastWarningTime { get; set; }

            public bool Add(HandleReferencesBase.Reference reference, int numberOfLoads)
            {
                EnsureCollectionsSync();

                if (ShouldAdd(numberOfLoads) == false)
                    return false;

                var referenceToAdd = new LoadedReference
                {
                    ReferenceId = reference.Key.ToString(),
                    NumberOfLoads = numberOfLoads
                };

                return Add(referenceToAdd);
            }

            public bool Add(LoadedReference reference)
            {
                EnsureCollectionsSync();

                if (ShouldAdd(reference.NumberOfLoads) == false)
                    return false;

                if (Top10LoadedReferences.TryGetValue(reference.ReferenceId, out LoadedReference existingReference))
                {
                    var indexOfValue = _sortedTop10.IndexOfValue(existingReference);
                    _sortedTop10.RemoveAt(indexOfValue);

                    existingReference.NumberOfLoads = reference.NumberOfLoads;

                    _sortedTop10.Add(existingReference.NumberOfLoads, existingReference);
                }
                else
                {
                    if (Top10LoadedReferences.TryAdd(reference.ReferenceId, reference))
                    {
                        _sortedTop10.Add(reference.NumberOfLoads, reference);
                    }
                }

                while (_sortedTop10.Count > MaxNumberOfDetailsPerIndex)
                {
                    LoadedReference lowest = _sortedTop10.Values[0];

                    Top10LoadedReferences.Remove(lowest.ReferenceId);

                    _sortedTop10.RemoveAt(0);
                }

                Debug.Assert(_sortedTop10.Count == Top10LoadedReferences.Count, "_sortedTop10.Count == Top10LoadedReferences.Count");

                return true;
            }

            private void EnsureCollectionsSync()
            {
                if (_sortedTop10.Count == Top10LoadedReferences.Count) 
                    return;

                _sortedTop10.Clear();

                foreach ((_, LoadedReference reference) in Top10LoadedReferences)
                {
                    _sortedTop10[reference.NumberOfLoads] = reference;
                }
            }

            private bool ShouldAdd(int numberOfLoads)
            {
                if (_sortedTop10.Count < MaxNumberOfDetailsPerIndex)
                    return true;

                if (numberOfLoads <= _sortedTop10.Keys[0])
                    return false;

                return true;
            }
        }

        public sealed class LoadedReference
        {
            public string ReferenceId { get; set; }

            public int NumberOfLoads { get; set; }
        }

        private sealed class AllowDuplicatedNumberOfLoadsComparer : IComparer<int>
        {
            public static AllowDuplicatedNumberOfLoadsComparer Instance = new AllowDuplicatedNumberOfLoadsComparer();

            public int Compare(int x, int y)
            {
                if (x == y)
                    return 1; // to allow duplicates

                return x - y;
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Sparrow.Utils;

namespace Raven.Server.Utils
{
    public enum ConflictStatus
    {
        Update,
        Conflict,
        AlreadyMerged,
    }

    public static class ChangeVectorUtils
    {
        public static ConflictStatus GetConflictStatus(ChangeVector remoteChangeVector, ChangeVector localChangeVector, HashSet<string> exclude = null, ChangeVectorMode mode = ChangeVectorMode.Version)
        {
            if (mode == ChangeVectorMode.Order)
            {
                remoteChangeVector = remoteChangeVector.Order;
                localChangeVector = localChangeVector.Order;
            }
            else
            {
                remoteChangeVector = remoteChangeVector.Version;
                localChangeVector = localChangeVector.Version;
            }

            return GetConflictStatus(remoteChangeVector.AsString(), localChangeVector.AsString(), exclude);
        }

        public static ConflictStatus GetConflictStatus(string remoteAsString, string localAsString, HashSet<string> exclude = null)
        {
            var remote = ChangeVectorParts.GetPart(remoteAsString.AsSpan(), ChangeVectorPart.Version);
            var local = ChangeVectorParts.GetPart(localAsString.AsSpan(), ChangeVectorPart.Version);

            if (remote.SequenceEqual(local))
                return ConflictStatus.AlreadyMerged;

            if (remote.Length == 0)
                return ConflictStatus.AlreadyMerged;

            if (local.Length == 0)
                return ConflictStatus.Update;

            //any missing entries from a change vector are assumed to have zero value
            var localHasLargerEntries = false;
            var remoteHasLargerEntries = false;

            var localLength = CountEntries(local);
            int numOfMatches = 0;
            var remoteEnumerator = new ChangeVectorEnumerator(remote);
            while (remoteEnumerator.MoveNext())
            {
                bool found = false;

                if (ContainsExcluded(exclude, remoteEnumerator.DbId))
                    continue;

                var localEnumerator = new ChangeVectorEnumerator(local);
                while (localEnumerator.MoveNext())
                {
                    if (ContainsExcluded(exclude, localEnumerator.DbId))
                        continue;

                    if (remoteEnumerator.DbId.SequenceEqual(localEnumerator.DbId))
                    {
                        found = true;
                        numOfMatches++;

                        if (remoteEnumerator.Etag > localEnumerator.Etag)
                        {
                            remoteHasLargerEntries = true;
                        }
                        else if (remoteEnumerator.Etag < localEnumerator.Etag)
                        {
                            localHasLargerEntries = true;
                        }
                        break;
                    }
                }
                if (found == false)
                {
                    remoteHasLargerEntries = true;
                }
            }
            if (numOfMatches < local.Length)
            {
                localHasLargerEntries = true;
            }

            if (remoteHasLargerEntries && localHasLargerEntries)
                return ConflictStatus.Conflict;

            if (remoteHasLargerEntries == false && localHasLargerEntries == false)
                return ConflictStatus.AlreadyMerged; // change vectors identical

            return remoteHasLargerEntries ? ConflictStatus.Update : ConflictStatus.AlreadyMerged;

            static int CountEntries(ReadOnlySpan<char> changeVector)
            {
                var count = 0;
                var enumerator = new ChangeVectorEnumerator(changeVector);
                while (enumerator.MoveNext())
                    count++;

                return count;
            }

            static bool ContainsExcluded(HashSet<string> exclude, ReadOnlySpan<char> dbId)
            {
                if (exclude == null)
                    return false;

                return exclude.TryGetAlternateLookup<ReadOnlySpan<char>>(out var lookup)
                    ? lookup.Contains(dbId)
                    : exclude.Contains(dbId.ToString());
            }
        }

        [ThreadStatic] private static StringBuilder _changeVectorBuffer;
        [ThreadStatic] private static List<ChangeVectorIndexEntry> _changeVectorIndexBufferA;
        [ThreadStatic] private static List<ChangeVectorIndexEntry> _changeVectorIndexBufferB;

        static ChangeVectorUtils()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () =>
            {
                _changeVectorBuffer = null;
                _changeVectorIndexBufferA = null;
                _changeVectorIndexBufferB = null;
            };
        }

        private static int NumberOfDigits(long etag)
        {
            int count = 0;
            do
            {
                count++;
                etag /= 10;
            } while (etag != 0);
            return count;
        }

        private static long ParseToLong(string s, int start, int len)
        {
            long num;
            num = s[start] - '0';
            for (int i = 1; i < len; i++)
            {
                num *= 10;
                num += s[start + i] - '0';
            }
            return num;
        }

        public static (bool IsValid, string ChangeVector) TryUpdateChangeVector(DocumentDatabase database, ChangeVector oldChangeVector, long? etag = null)
        {
            if (etag == null)
            {
                etag = database.DocumentsStorage.GenerateNextEtag();
            }

            return TryUpdateChangeVector(database.ServerStore.NodeTag, database.DbBase64Id, etag.Value, oldChangeVector);
        }

        public static (bool IsValid, string ChangeVector) TryUpdateChangeVector(string nodeTag, string dbIdInBase64, long etag, ChangeVector changeVector)
        {
            InitializeThreadLocalState();
            var oldChangeVector = changeVector.AsString();

            Debug.Assert(oldChangeVector != null);

            // PERF: Avoid paying the threadstatic sync code every time. 
            var vectorBuffer = _changeVectorBuffer;

            var dbIndex = oldChangeVector.IndexOf(dbIdInBase64, StringComparison.Ordinal);
            if (dbIndex < 0)
            {
                vectorBuffer.Append(nodeTag)
                    .Append(':')
                    .Append(etag)
                    .Append('-')
                    .Append(dbIdInBase64);

                if (string.IsNullOrEmpty(oldChangeVector) == false)
                {
                    vectorBuffer.Append(", ").Append(oldChangeVector);
                    // we need to maintain the dbId order
                    return (true, vectorBuffer.ToString().ToChangeVector().SerializeVector());
                }

                return (true, vectorBuffer.ToString());
            }

            int newEtagSize = NumberOfDigits(etag);

            var existingEtagEndIndex = dbIndex - 1;
            var currentEtagStartIndex = oldChangeVector.LastIndexOf(':', existingEtagEndIndex) + 1;

            var existingLen = existingEtagEndIndex - currentEtagStartIndex;
            var existingEtag = ParseToLong(oldChangeVector, currentEtagStartIndex, existingLen);
            // assume no trailing zeros
            var diff = newEtagSize - existingLen;
            if (diff == 0)
            {
                // compare the strings instead of parsing to int
                if (existingEtag >= etag)
                {
                    //nothing to do
                    return (false, null);
                }
                // we clone the string because others might hold a reference to it and consider it immutable
                vectorBuffer.Append(oldChangeVector);

                // replace the etag

                Format.Backwards.WriteNumber(vectorBuffer, currentEtagStartIndex + newEtagSize - 1, etag);
                return (true, vectorBuffer.ToString());
            }

            if (diff < 0)
            {
                // nothing to do, already known to be smaller
                return (false, null);
            }

            // allocate new string
            vectorBuffer.Append(oldChangeVector, 0, currentEtagStartIndex)
                .Append(etag)
                .Append(oldChangeVector, existingEtagEndIndex, oldChangeVector.Length - existingEtagEndIndex);
 
            return (true, vectorBuffer.ToString());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void InitializeThreadLocalState()
        {
            if (_changeVectorBuffer == null)
                _changeVectorBuffer = new StringBuilder();
            _changeVectorBuffer.Length = 0;
        }

        public static string MergeVectors(string vectorAstring, string vectorBstring)
        {
            return ChangeVectorMerger.Merge(vectorAstring, vectorBstring);
        }

        public static ChangeVector NewChangeVector(DocumentDatabase database, long etag, IChangeVectorOperationContext context)
        {
            return context.GetChangeVector(NewChangeVector(database.ServerStore.NodeTag, etag, database.DbBase64Id));
        }

        public static string NewChangeVector(string nodeTag, long etag, string dbIdInBase64)
        {
            InitializeThreadLocalState();

            return _changeVectorBuffer
                .Append(nodeTag)
                .Append(':')
                .Append(etag)
                .Append('-')
                .Append(dbIdInBase64)
                .ToString();
        }

        public static long GetOrderEtagById(string changeVector, string id) => ChangeVectorStringReader.GetOrderEtagById(changeVector, id);

        public static long GetVersionEtagById(string changeVector, string id) => ChangeVectorStringReader.GetVersionEtagById(changeVector, id);

        public static string GetOrderNodeTagById(string changeVector, string id) => ChangeVectorStringReader.GetOrderNodeTagById(changeVector, id);

        internal static string GetVersionNodeTagById(string changeVector, string id) => ChangeVectorStringReader.GetVersionNodeTagById(changeVector, id);

        public static long Distance(string changeVectorA, string changeVectorB)
        {
            var a = _changeVectorIndexBufferA ??= [];
            var b = _changeVectorIndexBufferB ??= [];
            a.Clear();
            b.Clear();

            FillChangeVectorIndexEntries(changeVectorA, a);
            FillChangeVectorIndexEntries(changeVectorB, b);

            if (a.Count == 0 && b.Count == 0)
                return 0;

            if (a.Count == 0)
                return -ConsumeRest(b, 0);

            if (b.Count == 0)
                return ConsumeRest(a, 0);

            CollectionsMarshal.AsSpan(a).Sort(new ChangeVectorIndexEntryComparer(changeVectorA));
            CollectionsMarshal.AsSpan(b).Sort(new ChangeVectorIndexEntryComparer(changeVectorB));

            var aIndex = 0;
            var bIndex = 0;
            var diff = 0L;

            while (true)
            {
                if (aIndex == a.Count)
                    return diff - ConsumeRest(b, bIndex);

                if (bIndex == b.Count)
                    return diff + ConsumeRest(a, aIndex);

                var aElement = a[aIndex];
                var bElement = b[bIndex];

                var compare = aElement.GetDbId(changeVectorA).CompareTo(bElement.GetDbId(changeVectorB), StringComparison.Ordinal);
                
                if (compare == 0)
                {
                    diff += aElement.Etag - bElement.Etag;
                    aIndex++;
                    bIndex++;
                }
                else if (compare < 0)
                {
                    diff += aElement.Etag;
                    aIndex++;
                }
                else
                {
                    diff -= bElement.Etag;
                    bIndex++;
                }
            }
        }

        private static void FillChangeVectorIndexEntries(string changeVector, List<ChangeVectorIndexEntry> entries)
        {
            var version = GetVersionSpan(changeVector, out var versionStart);
            var enumerator = new ChangeVectorEnumerator(version);
            while (enumerator.MoveNext())
                entries.Add(new ChangeVectorIndexEntry(enumerator.Etag, versionStart + enumerator.DbIdStart, enumerator.DbIdLength));
        }

        private static ReadOnlySpan<char> GetVersionSpan(string changeVector, out int versionStart)
        {
            var changeVectorSpan = changeVector.AsSpan();
            var separatorIndex = ChangeVectorParts.GetCompositeSeparatorIndex(changeVectorSpan);
            if (separatorIndex < 0)
            {
                versionStart = 0;
                return changeVectorSpan;
            }

            versionStart = separatorIndex + 1;
            return changeVectorSpan.Slice(versionStart);
        }

        private static long ConsumeRest(List<ChangeVectorIndexEntry> changeVectorEntries, in int index)
        {
            var rest = 0L;
            for (int i = index; i < changeVectorEntries.Count; i++)
            {
                rest += changeVectorEntries[i].Etag;
            }

            return rest;
        }

        private readonly struct ChangeVectorIndexEntry(long etag, int dbIdStart, int dbIdLength)
        {
            public readonly long Etag = etag;
            public ReadOnlySpan<char> GetDbId(string changeVector) => changeVector.AsSpan(dbIdStart, dbIdLength);
        }

        private readonly struct ChangeVectorIndexEntryComparer(string changeVector) : IComparer<ChangeVectorIndexEntry>
        {
            public int Compare(ChangeVectorIndexEntry x, ChangeVectorIndexEntry y)
            {
                return x.GetDbId(changeVector).CompareTo(y.GetDbId(changeVector), StringComparison.Ordinal);
            }
        }

        public static string GetClusterWideChangeVector(string databaseId, long prevCountPerShard, bool addTrxAddition, long index, string clusterTransactionId)
        {
            var stringBuilder = new StringBuilder(ChangeVectorParser.RaftTag)
                .Append(':').Append(prevCountPerShard)
                .Append('-').Append(databaseId);
            if (addTrxAddition)
            {
                stringBuilder
                    .Append(',').Append(ChangeVectorParser.TrxnTag)
                    .Append(':').Append(index)
                    .Append('-').Append(clusterTransactionId);
            }
            return stringBuilder.ToString();
        }
    }
}

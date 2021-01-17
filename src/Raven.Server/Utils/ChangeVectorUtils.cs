using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Lucene.Net.Support;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
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
        public static ConflictStatus GetConflictStatus(string remoteAsString, string localAsString)
        {
            if (remoteAsString == localAsString)
                return ConflictStatus.AlreadyMerged;

            if (string.IsNullOrEmpty(remoteAsString))
                return ConflictStatus.AlreadyMerged;

            if (string.IsNullOrEmpty(localAsString))
                return ConflictStatus.Update;

            var local = localAsString.ToChangeVector();
            var remote = remoteAsString.ToChangeVector();

            //any missing entries from a change vector are assumed to have zero value
            var localHasLargerEntries = false;
            var remoteHasLargerEntries = false;

            int numOfMatches = 0;
            for (int i = 0; i < remote.Length; i++)
            {
                bool found = false;

                for (int j = 0; j < local.Length; j++)
                {
                    if (remote[i].DbId == local[j].DbId)
                    {
                        found = true;
                        numOfMatches++;

                        if (remote[i].Etag > local[j].Etag)
                        {
                            remoteHasLargerEntries = true;
                        }
                        else if (remote[i].Etag < local[j].Etag)
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
        }

        [ThreadStatic] private static StringBuilder _changeVectorBuffer;

        static ChangeVectorUtils()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () =>
            {
                _changeVectorBuffer = null;
                _mergeVectorBuffer = null;
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

        public static (bool IsValid, string ChangeVector) TryUpdateChangeVector(DocumentDatabase database, string oldChangeVector, long? etag = null)
        {
            if (etag == null)
            {
                etag = database.DocumentsStorage.GenerateNextEtag();
            }

            return TryUpdateChangeVector(database.ServerStore.NodeTag, database.DbBase64Id, etag.Value, oldChangeVector);
        }

        public static (bool IsValid, string ChangeVector) TryUpdateChangeVector(string nodeTag, string dbIdInBase64, long etag, string oldChangeVector)
        {
            InitializeThreadLocalState();

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

        public static void ThrowConflictingEtag(string id, string changeVector, long newEtag, string dbId, string nodeTag)
        {
            throw new InvalidOperationException($"Tried to update the change vector '{changeVector}' but the new etag '{newEtag}' is smaller than " +
                                                $"the etag in the change vector. DocumentId= '{id}', DatabaseId='{dbId}', NodeTag='{nodeTag}'.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void InitializeThreadLocalState()
        {
            if (_changeVectorBuffer == null)
                _changeVectorBuffer = new StringBuilder();
            _changeVectorBuffer.Length = 0;
        }

        [ThreadStatic] private static List<ChangeVectorEntry> _mergeVectorBuffer;


        public static string MergeVectors(string vectorAstring, string vectorBstring)
        {
            if (string.IsNullOrEmpty(vectorAstring))
                return vectorBstring;
            if (string.IsNullOrEmpty(vectorBstring))
                return vectorAstring;

            if (_mergeVectorBuffer == null)
                _mergeVectorBuffer = new EquatableList<ChangeVectorEntry>();
            _mergeVectorBuffer.Clear();

            ChangeVectorParser.MergeChangeVector(vectorAstring, _mergeVectorBuffer);
            ChangeVectorParser.MergeChangeVector(vectorBstring, _mergeVectorBuffer);

            return _mergeVectorBuffer.SerializeVector();
        }

        public static string MergeVectors(List<string> changeVectors)
        {
            if (_mergeVectorBuffer == null)
                _mergeVectorBuffer = new EquatableList<ChangeVectorEntry>();
            _mergeVectorBuffer.Clear();

            for (int i = 0; i < changeVectors.Count; i++)
            {
                ChangeVectorParser.MergeChangeVector(changeVectors[i], _mergeVectorBuffer);
            }

            return _mergeVectorBuffer.SerializeVector();
        }

        public static string NewChangeVector(DocumentDatabase database, long etag)
        {
            return NewChangeVector(database.ServerStore.NodeTag, etag, database.DbBase64Id);
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

        public static long GetEtagById(string changeVector, string id)
        {
            if (changeVector == null)
                return 0;

            var index = changeVector.IndexOf("-" + id, StringComparison.Ordinal);
            if (index == -1)
                return 0;

            var end = index - 1;
            var start = changeVector.LastIndexOf(":", end, StringComparison.Ordinal) + 1;

            return long.Parse(changeVector.Substring(start, end - start + 1));
        }

        public static string GetNodeTagById(string changeVector, string id)
        {
            if (changeVector == null)
                return null;

            var indexOfId = changeVector.IndexOf("-" + id, StringComparison.Ordinal);
            if (indexOfId < 1)
                return null;

            var endOfNodeTag = changeVector.LastIndexOf(":", indexOfId - 1, StringComparison.Ordinal);
            if (endOfNodeTag < 1)
                return null;

            var start = changeVector.LastIndexOf(", ", endOfNodeTag - 1, StringComparison.OrdinalIgnoreCase) + 1;

            return changeVector.Substring(start, endOfNodeTag - start);
        }

        public static long Distance(string changeVectorA, string changeVectorB)
        {
            var a = changeVectorA?.ToChangeVectorList();
            var b = changeVectorB?.ToChangeVectorList();

            if (a == null && b == null)
                return 0;

            if (a == null)
                return -ConsumeRest(b, 0);

            if (b == null)
                return ConsumeRest(a, 0);

            a.Sort((x, y) => string.Compare(x.DbId, y.DbId, StringComparison.Ordinal));
            b.Sort((x, y) => string.Compare(x.DbId, y.DbId, StringComparison.Ordinal));

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

                var compare = string.Compare(aElement.DbId, bElement.DbId, StringComparison.Ordinal);
                
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

        private static long ConsumeRest(List<ChangeVectorEntry> changeVectorEntries, in int index)
        {
            var rest = 0L;
            for (int i = index; i < changeVectorEntries.Count; i++)
            {
                rest += changeVectorEntries[i].Etag;
            }

            return rest;
        }
    }
}

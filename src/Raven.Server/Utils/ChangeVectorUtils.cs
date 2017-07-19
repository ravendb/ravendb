using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Raven.Client.Util;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Utils
{
    public enum ConflictStatus
    {
        Update,
        Conflict,
        AlreadyMerged
    }

    public static class ChangeVectorUtils
    {
        public static Dictionary<Guid, long> ParseChangeVectorToDictionary(this string changeVector)
        {
            var dic = new Dictionary<Guid,long>();
            foreach (var entry in changeVector.ToChangeVector())
            {
                if (dic.ContainsKey(entry.DbId))
                {
                    throw new InvalidDataException("Duplicated entry!");
                }
                dic.Add(entry.DbId,entry.Etag);
            }
            return dic;
        }

        public static string FormatToChangeVector(string nodeTag, long etag, Guid dbId)
        {
            return $"{nodeTag}:{etag}-{Convert.ToBase64String(dbId.ToByteArray())}";
        }

        public static LazyStringValue NewChangeVector(DocumentsOperationContext ctx, string nodeTag, long etag, Guid dbId)
        {
            return ctx.GetLazyString($"{nodeTag}:{etag}-{Convert.ToBase64String(dbId.ToByteArray())}");
        }

        public static ConflictStatus GetConflictStatus(string remote, string local)
        {
            throw new NotImplementedException();
            //            if (local == null)
            //                return ConflictStatus.Update;
            //
            //            //any missing entries from a change vector are assumed to have zero value
            //            var remoteHasLargerEntries = local.Length < remote.Length;
            //            var localHasLargerEntries = remote.Length < local.Length;
            //
            //            Array.Sort(remote); // todo: check if we need this
            //            Array.Sort(local); // todo: check if we need this
            //
            //            var localIndex = 0;
            //            var remoteIndex = 0;
            //
            //            while (localIndex < local.Length && remoteIndex < remote.Length)
            //            {
            //                var compareResult = remote[remoteIndex].DbId.CompareTo(local[localIndex].DbId);
            //                if (compareResult == 0)
            //                {
            //                    remoteHasLargerEntries |= remote[remoteIndex].Etag > local[localIndex].Etag;
            //                    localHasLargerEntries |= local[localIndex].Etag > remote[remoteIndex].Etag;
            //                    remoteIndex++;
            //                    localIndex++;
            //                }
            //                else if (compareResult > 0)
            //                {
            //                    localIndex++;
            //                    localHasLargerEntries = true;
            //                }
            //                else
            //                {
            //                    remoteIndex++;
            //                    remoteHasLargerEntries = true;
            //                }
            //
            //                if (localHasLargerEntries && remoteHasLargerEntries)
            //                    break;
            //            }
            //
            //            if (remoteIndex < remote.Length)
            //            {
            //                remoteHasLargerEntries = true;
            //            }
            //
            //            if (localIndex < local.Length)
            //            {
            //                localHasLargerEntries = true;
            //            }
            //
            //            if (remoteHasLargerEntries && localHasLargerEntries)
            //                return ConflictStatus.Conflict;
            //
            //            if (remoteHasLargerEntries == false && localHasLargerEntries == false)
            //                return ConflictStatus.AlreadyMerged; // change vectors identical
            //
            //            return remoteHasLargerEntries ? ConflictStatus.Update : ConflictStatus.AlreadyMerged;
        }


        public static LazyStringValue FuseChangeVectors(string cv1, string c2)
        {
            throw new NotImplementedException();
//            if (_changeVector.Length != changeVectorCount)
//                _changeVector = new ChangeVectorEntry[changeVectorCount];
//
//            for (int i = 0; i < changeVectorCount; i++)
//            {
//                _changeVector[i] = ((ChangeVectorEntry*)(buffer + position))[i];
//
//                if (maxReceivedChangeVectorByDatabase.TryGetValue(_changeVector[i].DbId, out long etag) == false
//                    || etag < _changeVector[i].Etag)
//                {
//                    maxReceivedChangeVectorByDatabase[_changeVector[i].DbId] = _changeVector[i].Etag;
//                }
//            }
        }

        public static bool TryUpdateChangeVector(Guid dbId, long etag, ref LazyStringValue changeVector)
        {
            throw new NotImplementedException();
            //            Debug.Assert(LastDatabaseChangeVector != null);
            //            var length = LastDatabaseChangeVector.Length;
            //            for (var i = 0; i < length; i++)
            //            {
            //                if (dbId != LastDatabaseChangeVector[i].DbId)
            //                    continue;
            //
            //                if (LastDatabaseChangeVector[i].Etag <= etag)
            //                    return false;
            //
            //                LastDatabaseChangeVector[i].Etag = etag;
            //                return true;
            //            }
            //
            //            Array.Resize(ref LastDatabaseChangeVector, length + 1);
            //            LastDatabaseChangeVector[length] = new ChangeVectorEntry
            //            {
            //                DbId = dbId,
            //                Etag = etag
            //            };
            //            return true;
        }
        
        public static LazyStringValue MergeVectors(string vectorA, string vectorB)
        {
            Debug.Assert(vectorA != null);
            Debug.Assert(vectorB != null);

            throw new NotImplementedException();
            //            Array.Sort(vectorA);
            //            Array.Sort(vectorB);
            //            int ia = 0, ib = 0;
            //            var merged = new List<ChangeVectorEntry>();
            //            while (ia < vectorA.Length && ib < vectorB.Length)
            //            {
            //                int res = vectorA[ia].DbId.CompareTo(vectorB[ib].DbId);
            //                if (res == 0)
            //                {
            //                    merged.Add(new ChangeVectorEntry
            //                    {
            //                        DbId = vectorA[ia].DbId,
            //                        Etag = Math.Max(vectorA[ia].Etag, vectorB[ib].Etag)
            //                    });
            //                    ia++;
            //                    ib++;
            //                }
            //                else if (res < 0)
            //                {
            //                    merged.Add(vectorA[ia]);
            //                    ia++;
            //                }
            //                else
            //                {
            //                    merged.Add(vectorB[ib]);
            //                    ib++;
            //                }
            //            }
            //            for (; ia < vectorA.Length; ia++)
            //            {
            //                merged.Add(vectorA[ia]);
            //            }
            //            for (; ib < vectorB.Length; ib++)
            //            {
            //                merged.Add(vectorB[ib]);
            //            }
            //            return merged.ToArray();
        }

        public static LazyStringValue MergeVectors(List<LazyStringValue> changeVectors)
        {
            // beware of duplicates
            throw new NotImplementedException();
            //            var mergedVector = new Dictionary<Guid, long>();
            //
            //            foreach (var changeVector in changeVectors)
            //            {
            //                foreach (var changeVectorEntry in changeVector)
            //                {
            //                    if (!mergedVector.ContainsKey(changeVectorEntry.DbId))
            //                    {
            //                        mergedVector[changeVectorEntry.DbId] = changeVectorEntry.Etag;
            //                    }
            //                    else
            //                    {
            //                        mergedVector[changeVectorEntry.DbId] = Math.Max(mergedVector[changeVectorEntry.DbId],
            //                            changeVectorEntry.Etag);
            //                    }
            //                }
            //            }
            //
            //            return mergedVector.Select(kvp => new ChangeVectorEntry
            //            {
            //                DbId = kvp.Key,
            //                Etag = kvp.Value
            //            }).ToArray();
        }
    }
}
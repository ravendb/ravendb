using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Raven.Server.Documents.Replication;

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

        public static ConflictStatus GetConflictStatus(string remoteAsString, string localAsString)
        {
            if (localAsString == null)
                return ConflictStatus.Update;

            var local = localAsString.ToChangeVector();
            var remote = remoteAsString.ToChangeVector();
            //any missing entries from a change vector are assumed to have zero value
            var remoteHasLargerEntries = local.Length < remote.Length;
            var localHasLargerEntries = remote.Length < local.Length;
            
            Array.Sort(remote); // todo: check if we need this
            Array.Sort(local); // todo: check if we need this
            
            var localIndex = 0;
            var remoteIndex = 0;
            
            while (localIndex < local.Length && remoteIndex < remote.Length)
            {
                var compareResult = remote[remoteIndex].DbId.CompareTo(local[localIndex].DbId);
                if (compareResult == 0)
                {
                    remoteHasLargerEntries |= remote[remoteIndex].Etag > local[localIndex].Etag;
                    localHasLargerEntries |= local[localIndex].Etag > remote[remoteIndex].Etag;
                    remoteIndex++;
                    localIndex++;
                }
                else if (compareResult > 0)
                {
                    localIndex++;
                    localHasLargerEntries = true;
                }
                else
                {
                    remoteIndex++;
                    remoteHasLargerEntries = true;
                }
            
                if (localHasLargerEntries && remoteHasLargerEntries)
                    break;
            }
            
            if (remoteIndex < remote.Length)
            {
                remoteHasLargerEntries = true;
            }
            
            if (localIndex < local.Length)
            {
                localHasLargerEntries = true;
            }
            
            if (remoteHasLargerEntries && localHasLargerEntries)
                return ConflictStatus.Conflict;
            
            if (remoteHasLargerEntries == false && localHasLargerEntries == false)
                return ConflictStatus.AlreadyMerged; // change vectors identical
            
            return remoteHasLargerEntries ? ConflictStatus.Update : ConflictStatus.AlreadyMerged;
        }

        public static bool TryUpdateChangeVector(Guid dbId, long etag, ref string changeVector)
        {
            Debug.Assert(changeVector != null);
            var cv = changeVector.ToChangeVector();
            var length = cv.Length;
            for (var i = 0; i < length; i++)
            {
                if (dbId != cv[i].DbId)
                    continue;
            
                if (cv[i].Etag >= etag)
                    return false;

                cv[i].Etag = etag;
                changeVector = cv.ToJson();
                return true;
            }
            
            Array.Resize(ref cv, length + 1);
            cv[length] = new ChangeVectorEntry
            {
                DbId = dbId,
                Etag = etag
            };
            changeVector = cv.ToJson();
            return true;
        }

        public static string MergeVectors(string vectorAstring, string vectorBstring)
        {
            if (vectorAstring == null)
                return vectorBstring;
            if (vectorBstring == null)
                return vectorAstring;

            var vectorA = vectorAstring.ToChangeVector();
            var vectorB = vectorBstring.ToChangeVector();

            Array.Sort(vectorA);
            Array.Sort(vectorB);
            int ia = 0, ib = 0;
            var merged = new List<ChangeVectorEntry>();
            while (ia < vectorA.Length && ib < vectorB.Length)
            {
                int res = vectorA[ia].DbId.CompareTo(vectorB[ib].DbId);
                if (res == 0)
                {
                    merged.Add(new ChangeVectorEntry
                    {
                        DbId = vectorA[ia].DbId,
                        Etag = Math.Max(vectorA[ia].Etag, vectorB[ib].Etag)
                    });
                    ia++;
                    ib++;
                }
                else if (res < 0)
                {
                    merged.Add(vectorA[ia]);
                    ia++;
                }
                else
                {
                    merged.Add(vectorB[ib]);
                    ib++;
                }
            }
            for (; ia < vectorA.Length; ia++)
            {
                merged.Add(vectorA[ia]);
            }
            for (; ib < vectorB.Length; ib++)
            {
                merged.Add(vectorB[ib]);
            }
            return merged.ToArray().ToJson();
        }

        public static string MergeVectors(List<LazyStringValue> changeVectors)
        {
            var mergedVector = new Dictionary<Guid, long>();
            
            foreach (var changeVector in changeVectors)
            {
                foreach (var changeVectorEntry in changeVector.ToString().ToChangeVector())
                {
                    if (!mergedVector.ContainsKey(changeVectorEntry.DbId))
                    {
                        mergedVector[changeVectorEntry.DbId] = changeVectorEntry.Etag;
                    }
                    else
                    {
                        mergedVector[changeVectorEntry.DbId] = Math.Max(mergedVector[changeVectorEntry.DbId],
                            changeVectorEntry.Etag);
                    }
                }
            }
            
            var merged = mergedVector.Select(kvp => new ChangeVectorEntry
            {
                DbId = kvp.Key,
                Etag = kvp.Value
            }).ToArray();

            return merged.ToJson();
        }
    }
}
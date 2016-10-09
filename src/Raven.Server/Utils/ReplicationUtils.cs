using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents;
using Raven.Server.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.BTrees;

namespace Raven.Server.Utils
{
    public static class ReplicationUtils
    {
        public static ChangeVectorEntry[] UpdateChangeVectorWithNewEtag(Guid dbId, long newEtag, ChangeVectorEntry[] changeVector)
        {
            var length = changeVector.Length;
            for (int i = 0; i < length; i++)
            {
                if (changeVector[i].DbId == dbId)
                {
                    changeVector[i].Etag = newEtag;
                    return changeVector;
                }
            }
            Array.Resize(ref changeVector, length + 1);
            changeVector[length].DbId = dbId;
            changeVector[length].Etag = newEtag;
            return changeVector;
        }

        public static unsafe void WriteChangeVectorTo(JsonOperationContext context, Dictionary<Guid, long> changeVector, Tree tree)
        {
            foreach (var kvp in changeVector)
            {
                var dbId = kvp.Key;
                var etagBigEndian = IPAddress.HostToNetworkOrder(kvp.Value);
                Slice key;
                Slice value;
                using (Slice.External(context.Allocator, (byte*) &dbId, sizeof(Guid), out key))
                using (Slice.External(context.Allocator, (byte*) &etagBigEndian, sizeof(long), out value))
                    tree.Add(key, value);
            }
        }

        public static unsafe void WriteChangeVectorTo(JsonOperationContext context, ChangeVectorEntry[] changeVector, Tree tree)
        {
            foreach (var item in changeVector)
            {
                var dbId = item.DbId;
                var etagBigEndian = IPAddress.HostToNetworkOrder(item.Etag);
                Slice key;
                Slice value;
                using (Slice.External(context.Allocator, (byte*)&dbId, sizeof(Guid), out key))
                using (Slice.External(context.Allocator, (byte*)&etagBigEndian, sizeof(long), out value))
                    tree.Add(key, value);
            }

        }

        public static unsafe ChangeVectorEntry[] ReadChangeVectorFrom(Tree tree)
        {
            var changeVector = new ChangeVectorEntry[tree.State.NumberOfEntries];
            using (var iter = tree.Iterate(false))
            {
                if (iter.Seek(Slices.BeforeAllKeys) == false)
                    return changeVector;
                var buffer = new byte[sizeof(Guid)];
                int index = 0;
                do
                {
                    var read = iter.CurrentKey.CreateReader().Read(buffer, 0, sizeof(Guid));
                    if (read != sizeof(Guid))
                        throw new InvalidDataException($"Expected guid, but got {read} bytes back for change vector");

                    changeVector[index].DbId = new Guid(buffer);
                    changeVector[index].Etag = iter.CreateReaderForCurrent().ReadBigEndianInt64();
                    index++;
                } while (iter.MoveNext());
            }
            return changeVector;
        }

        public static ChangeVectorEntry[] MergeVectors(ChangeVectorEntry[] vectorA, ChangeVectorEntry[] vectorB)
        {
            var merged = new ChangeVectorEntry[Math.Max(vectorA.Length, vectorB.Length)];
            var inx = 0;
            var largerVector = (vectorA.Length >= vectorB.Length) ? vectorA : vectorB;
            var smallerVector = (largerVector == vectorA) ? vectorB : vectorA;
            foreach (var entryA in largerVector)
            {
                var etagA = entryA.Etag;
                ChangeVectorEntry first = new ChangeVectorEntry();
                foreach (var e in smallerVector)
                {
                    if (e.DbId == entryA.DbId)
                    {
                        first = e;
                        break;
                    }
                }
                var etagB = first.Etag;

                merged[inx++] = new ChangeVectorEntry
                {
                    DbId = entryA.DbId,
                    Etag = Math.Max(etagA,etagB)
                };
            }
            return merged;
        }

        public static DynamicJsonValue GetJsonForConflicts(string docId, IEnumerable<DocumentConflict> conflicts)
        {
            var conflictsArray = new DynamicJsonArray();
            foreach (var c in conflicts)
            {
                conflictsArray.Add(new DynamicJsonValue
                {
                    ["ChangeVector"] = c.ChangeVector.ToJson(),
                    ["Doc"] = c.Doc
                });
            }

            return new DynamicJsonValue
            {
                ["Message"] = "Conflict detected on " + docId + ", conflict must be resolved before the document will be accessible",
                ["DocId"] = docId,
                ["Conflics"] = conflictsArray
            };
        }

    }
}

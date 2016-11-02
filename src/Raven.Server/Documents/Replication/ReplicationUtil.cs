using System;
using System.IO;
using System.Net;
using Raven.Client.Replication.Messages;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Replication
{
    public static unsafe class ReplicationUtil
    {
        public static TEnum GetEnumFromTableValueReader<TEnum>(TableValueReader tvr, int index)
        {
            int size;
            var storageTypeNum = *(int*)tvr.Read(index, out size);
            return (TEnum)Enum.ToObject(typeof(TEnum), storageTypeNum);
        }

        public static ChangeVectorEntry[] GetChangeVectorEntriesFromTableValueReader(TableValueReader tvr, int index)
        {
            int size;
            var pChangeVector = (ChangeVectorEntry*)tvr.Read(index, out size);
            var changeVector = new ChangeVectorEntry[size / sizeof(ChangeVectorEntry)];
            for (int i = 0; i < changeVector.Length; i++)
            {
                changeVector[i] = pChangeVector[i];
            }
            return changeVector;
        }

        public static ChangeVectorEntry[] ReadChangeVectorFrom(Tree tree)
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
                var first = new ChangeVectorEntry();
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
                    Etag = Math.Max(etagA, etagB)
                };
            }
            return merged;
        }

        public static void WriteChangeVectorTo(TransactionOperationContext context, ChangeVectorEntry[] changeVector, Tree tree)
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
    }
}

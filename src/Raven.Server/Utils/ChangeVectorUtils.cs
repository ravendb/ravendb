using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Voron;
using Voron.Data.BTrees;

namespace Raven.Server.Utils
{
    public class ChangeVectorUtils
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ChangeVectorToString(Dictionary<Guid, long> changeVector)
        {
            var sb = new StringBuilder();
            foreach (var kvp in changeVector)
                sb.Append($"{kvp.Key}:{kvp.Value};");

            return sb.ToString();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ChangeVectorToString(ChangeVectorEntry[] changeVector)
        {
            var sb = new StringBuilder();
            foreach (var kvp in changeVector)
                sb.Append($"{kvp.DbId}:{kvp.Etag};");

            return sb.ToString();
        }

        public static unsafe void WriteChangeVectorTo(DocumentsOperationContext context, Dictionary<Guid, long> changeVector, Tree tree)
        {
            Guid dbId;
            long etagBigEndian;
            Slice keySlice;
            Slice valSlice;
            using (Slice.External(context.Allocator, (byte*)&dbId, sizeof(Guid), out keySlice))
            using (Slice.External(context.Allocator, (byte*)&etagBigEndian, sizeof(long), out valSlice))
            {
                foreach (var kvp in changeVector)
                {
                    dbId = kvp.Key;
                    etagBigEndian = Bits.SwapBytes(kvp.Value);
                    tree.Add(keySlice, valSlice);
                }
            }
        }

        public static unsafe void WriteChangeVectorTo(ByteStringContext context, Dictionary<Guid, long> changeVector, Tree tree)
        {
            Guid dbId;
            long etagBigEndian;
            Slice keySlice;
            Slice valSlice;
            using (Slice.External(context, (byte*)&dbId, sizeof(Guid), out keySlice))
            using (Slice.External(context, (byte*)&etagBigEndian, sizeof(long), out valSlice))
            {
                foreach (var kvp in changeVector)
                {
                    dbId = kvp.Key;
                    etagBigEndian = Bits.SwapBytes(kvp.Value);
                    tree.Add(keySlice, valSlice);
                }
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

        public static ChangeVectorEntry[] GetChangeVectorForWrite(ChangeVectorEntry[] existingChangeVector, Guid dbid, long etag)
        {
            if (existingChangeVector == null || existingChangeVector.Length == 0)
            {
                return new[]
                {
                    new ChangeVectorEntry
                    {
                        DbId = dbid,
                        Etag = etag
                    }
                };
            }

            return UpdateChangeVectorWithNewEtag(dbid, etag, existingChangeVector);
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

        public static ChangeVectorEntry[] MergeVectors(ChangeVectorEntry[] vectorA, ChangeVectorEntry[] vectorB)
        {
            Array.Sort(vectorA);
            Array.Sort(vectorB);
            int ia = 0, ib = 0;
            var merged = new List<ChangeVectorEntry>();
            while (ia < vectorA.Length && ib < vectorB.Length)
            {
                int res = vectorA[ia].CompareTo(vectorB[ib]);
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
            return merged.ToArray();
        }

        public static ChangeVectorEntry[] MergeVectors(IReadOnlyList<ChangeVectorEntry[]> changeVectors)
        {
            var mergedVector = new Dictionary<Guid, long>();

            foreach (var changeVector in changeVectors)
            {
                foreach (var changeVectorEntry in changeVector)
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

            return mergedVector.Select(kvp => new ChangeVectorEntry
            {
                DbId = kvp.Key,
                Etag = kvp.Value
            }).ToArray();
        }
    }
}
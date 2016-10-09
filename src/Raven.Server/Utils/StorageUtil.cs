using System;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.Indexes;
using Voron.Data.Tables;

namespace Raven.Server.Utils
{
    public static class StorageUtil
    {
        public static unsafe TEnum GetEnumFromTableValueReader<TEnum>(TableValueReader tvr, int index)
        {
            int size;
            var storageTypeNum = *(int*)tvr.Read(index, out size);
            return (TEnum)Enum.ToObject(typeof(TEnum), storageTypeNum);
        }

        public static unsafe ChangeVectorEntry[] GetChangeVectorEntriesFromTableValueReader(TableValueReader tvr, int index)
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
    }
}

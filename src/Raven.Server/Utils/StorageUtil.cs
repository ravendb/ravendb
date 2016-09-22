using Raven.Client.Replication.Messages;
using Voron.Data.Tables;

namespace Raven.Server.Utils
{
    public static class StorageUtil
    {
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

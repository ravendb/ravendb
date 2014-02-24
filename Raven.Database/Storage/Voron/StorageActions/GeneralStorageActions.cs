using System;

using Raven.Abstractions.Extensions;
using Raven.Database.Storage.Voron.Impl;
using Raven.Database.Util.Streams;

using Voron.Impl;

namespace Raven.Database.Storage.Voron.StorageActions
{
    public class GeneralStorageActions : StorageActionsBase, IGeneralStorageActions
    {
	    private const int PulseTreshold = 16 * 1024;
	    private readonly Table generalTable;
	    private readonly TableStorage storage;
		private readonly Reference<WriteBatch> writeBatch;

		private int maybePulseCount;

		public GeneralStorageActions(TableStorage storage,Table generalTable, Reference<WriteBatch> writeBatch, SnapshotReader snapshot, IBufferPool bufferPool)
            :base(snapshot, bufferPool)
		{
			this.storage = storage;
            this.generalTable = generalTable;
            this.writeBatch = writeBatch;
        }

        public long GetNextIdentityValue(string name)
        {
            if (string.IsNullOrEmpty(name)) 
				throw new ArgumentNullException("name");

            var lowerKeyName = name.ToLowerInvariant();
            if (!generalTable.Contains(Snapshot, lowerKeyName, writeBatch.Value))
            {
                generalTable.Add(writeBatch.Value, lowerKeyName, BitConverter.GetBytes((long) 1), expectedVersion: 0);
                return 1;
            }

            var readResult = generalTable.Read(Snapshot, lowerKeyName, writeBatch.Value);
            using (var stream = readResult.Reader.AsStream())
            {
                var newValue = stream.ReadInt64() + 1;

                generalTable.Add(writeBatch.Value, lowerKeyName, BitConverter.GetBytes(newValue), expectedVersion: readResult.Version);
                return newValue;
            }
        }

        public void SetIdentityValue(string name, long value)
        {
            if (String.IsNullOrWhiteSpace(name)) throw new ArgumentNullException("name");

            var lowerKeyName = name.ToLowerInvariant();
            generalTable.Add(writeBatch.Value, lowerKeyName, BitConverter.GetBytes(value));
        }

        public void PulseTransaction()
        {
			storage.Write(writeBatch.Value);
            writeBatch.Value.Dispose();
			writeBatch.Value = new WriteBatch {DisposeAfterWrite = writeBatch.Value.DisposeAfterWrite};
		}

		public void MaybePulseTransaction()
        {
			if (++maybePulseCount / 1000 == 0)
				return;

			if (writeBatch.Value.Size() >= PulseTreshold)
			{
				PulseTransaction();
				maybePulseCount = 0;
			}
        }
    }
}

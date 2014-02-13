using System.IO;
using Raven.Database.Storage.Voron.Impl;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Voron.Impl;
using Raven.Abstractions.Extensions;
using System.Threading;

namespace Raven.Database.Storage.Voron.StorageActions
{
    public class GeneralStorageActions : IGeneralStorageActions
    {
	    private const int PulseTreshold = 16 * 1024;
	    private readonly Table generalTable;
	    private readonly TableStorage storage;
		private readonly Reference<WriteBatch> writeBatch;
        private readonly SnapshotReader snapshot;

		private int maybePulseCount;

		public GeneralStorageActions(TableStorage storage,Table generalTable, Reference<WriteBatch> writeBatch, SnapshotReader snapshot)
		{
			this.storage = storage;
            this.generalTable = generalTable;
            this.writeBatch = writeBatch;
            this.snapshot = snapshot;
        }

        public long GetNextIdentityValue(string name)
        {
            if (string.IsNullOrEmpty(name)) 
				throw new ArgumentNullException("name");

            var lowerKeyName = name.ToLowerInvariant();
            if (!generalTable.Contains(snapshot, lowerKeyName, writeBatch.Value))
            {
                generalTable.Add(writeBatch.Value, lowerKeyName, BitConverter.GetBytes((long) 1), expectedVersion: 0);
                return 1;
            }

	        var readResult = generalTable.Read(snapshot, lowerKeyName, writeBatch.Value);
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

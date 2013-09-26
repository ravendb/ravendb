using System.IO;
using Raven.Database.Storage.Voron.Impl;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Voron.Impl;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Storage.Voron.StorageActions
{
    public class GeneralStorageActions : IGeneralStorageActions
    {
        private readonly Table generalTable;

        private readonly WriteBatch writeBatch;
        private readonly SnapshotReader snapshot;

        public GeneralStorageActions(Table generalTable, WriteBatch writeBatch, SnapshotReader snapshot)
        {
            this.generalTable = generalTable;
            this.writeBatch = writeBatch;
            this.snapshot = snapshot;
        }

        public long GetNextIdentityValue(string name)
        {
            if (string.IsNullOrEmpty(name)) 
				throw new ArgumentNullException("name");

            var lowerKeyName = name.ToLowerInvariant();
            if (!generalTable.Contains(snapshot, lowerKeyName, writeBatch))
            {
                generalTable.Add(writeBatch, lowerKeyName, BitConverter.GetBytes((long) 1));
                return 1;
            }
            
            using (var readResult = generalTable.Read(snapshot, lowerKeyName, writeBatch))
            {
                var newValue = readResult.Stream.ReadInt64() + 1;

                generalTable.Add(writeBatch, lowerKeyName, BitConverter.GetBytes(newValue));
                return newValue;
            }
        }

        public void SetIdentityValue(string name, long value)
        {
            if (String.IsNullOrWhiteSpace(name)) throw new ArgumentNullException("name");

            var lowerKeyName = name.ToLowerInvariant();
            generalTable.Add(writeBatch, lowerKeyName, BitConverter.GetBytes(value));
        }

        public void PulseTransaction()
        {
        }

        public void MaybePulseTransaction()
        {
        }
    }
}

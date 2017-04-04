using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Storage.Voron.Impl;

using Voron;
using Voron.Impl;
using Voron.Trees;

namespace Raven.Database.Storage.Voron.StorageActions
{
    internal class GeneralStorageActions : StorageActionsBase, IGeneralStorageActions
    {
        private const int PulseTreshold = 16 * 1024 * 1024; // 16 MB
        private static readonly ILog logger = LogManager.GetCurrentClassLogger();
        private readonly TableStorage storage;
        private readonly Reference<WriteBatch> writeBatch;
        private readonly Reference<SnapshotReader> snapshot;
        private readonly StorageActionsAccessor storageActionsAccessor;
        private int maybePulseCount;

        public GeneralStorageActions(TableStorage storage, Reference<WriteBatch> writeBatch, Reference<SnapshotReader> snapshot, IBufferPool bufferPool, StorageActionsAccessor storageActionsAccessor)
            : base(snapshot, bufferPool)
        {
            this.storage = storage;
            this.writeBatch = writeBatch;
            this.snapshot = snapshot;
            this.storageActionsAccessor = storageActionsAccessor;
        }

        public IEnumerable<KeyValuePair<string, long>> GetIdentities(int start, int take, out long totalCount)
        {
            totalCount = storage.GetEntriesCount(storage.General);
            if (totalCount <= 0)
                return Enumerable.Empty<KeyValuePair<string, long>>();

            using (var iterator = storage.General.Iterate(Snapshot, writeBatch.Value))
            {
                if (iterator.Seek(Slice.BeforeAllKeys) == false || iterator.Skip(start) == false)
                    return Enumerable.Empty<KeyValuePair<string, long>>();

                var results = new List<KeyValuePair<string, long>>(); 

                do
                {
                    var identityName = iterator.CurrentKey.ToString();
                    var identityValue = iterator.CreateReaderForCurrent().ReadLittleEndianInt64();

                    results.Add(new KeyValuePair<string, long>(identityName, identityValue));
                }
                while (iterator.MoveNext() && results.Count < take);

                return results;
            }
        }

        public long GetNextIdentityValue(string name, int val)
        {
            if (string.IsNullOrEmpty(name)) 
                throw new ArgumentNullException("name");

            var lowerKeyName = (Slice)name.ToLowerInvariant();

            var readResult = storage.General.Read(Snapshot, lowerKeyName, writeBatch.Value); 
            if (readResult == null)
            {
                if (val == 0)
                    return 0;
                storage.General.Add(writeBatch.Value, lowerKeyName, BitConverter.GetBytes((long)val), expectedVersion: 0);
                return val;
            }

            using (var stream = readResult.Reader.AsStream())
            {
                long existingValue = stream.ReadInt64();
                if (val == 0)
                    return val;
                var newValue = existingValue + val;

                storage.General.Add(writeBatch.Value, lowerKeyName, BitConverter.GetBytes(newValue), expectedVersion: readResult.Version);
                return newValue;
            }
        }

        public void SetIdentityValue(string name, long value)
        {
            if (String.IsNullOrWhiteSpace(name)) throw new ArgumentNullException("name");

            var lowerKeyName = name.ToLowerInvariant();
            storage.General.Add(writeBatch.Value, lowerKeyName, BitConverter.GetBytes(value));
        }

        public void PulseTransaction()
        {
            try
            {
                storageActionsAccessor.ExecuteBeforeStorageCommit();

                storage.Write(writeBatch.Value);

                writeBatch.Value.Dispose();

                var hasOpenedIterators = snapshot.Value.HasOpenedIterators;

                snapshot.Value.Dispose();
                if (hasOpenedIterators)
                {
                    throw new InvalidOperationException("Cannot pulse transaction when we have iterators opened");
                }                
                
                snapshot.Value = storage.CreateSnapshot();

                writeBatch.Value = new WriteBatch {DisposeAfterWrite = writeBatch.Value.DisposeAfterWrite};
            }
            finally
            {
                storageActionsAccessor.ExecuteAfterStorageCommit();
            }
        }

        public bool MaybePulseTransaction(IIterator before, int addToPulseCount = 1, Action beforePulseTransaction = null)
        {
            var increment = Interlocked.Add(ref maybePulseCount, addToPulseCount);
            if (increment < 1024)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref maybePulseCount, 0, increment) != increment)
            {
                return false;
            }

            if (writeBatch.Value.Size() >= PulseTreshold)
            {
               if (before != null)
                    before.Dispose();

                if (beforePulseTransaction != null)
                    beforePulseTransaction();
                if (logger.IsDebugEnabled)
                    logger.Debug("MaybePulseTransaction() --> PulseTransaction()");
                PulseTransaction();
                return true;
            }
            return false;
        }

        public bool MaybePulseTransaction(int addToPulseCount = 1, Action beforePulseTransaction = null)
        {
            return MaybePulseTransaction(null, addToPulseCount, beforePulseTransaction);
        }
    }
}

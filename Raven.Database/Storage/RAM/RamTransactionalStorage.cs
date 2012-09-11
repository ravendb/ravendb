using System;
using System.Data;
using System.Threading;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Plugins;

namespace Raven.Database.Storage.RAM
{
	public class RamTransactionalStorage : ITransactionalStorage
	{
		private readonly ThreadLocal<RamStorageActionsAccessor> current = new ThreadLocal<RamStorageActionsAccessor>();

		private IUuidGenerator theGenerator;
		private InMemoryRavenConfiguration configuration;
		private readonly Action onCommit;
		private readonly RamState state = new RamState();

		public void Dispose()
		{
			
		}

		public Guid Id { get; private set; }


		public RamTransactionalStorage(InMemoryRavenConfiguration configuration, Action onCommit)
		{
			this.configuration = configuration;
			this.onCommit = onCommit;
		}

		public void Batch(Action<IStorageActionsAccessor> action)
		{
			if (current.Value != null)
			{
				action(current.Value);
				return;
			}

			InMemoryTransaction.Begin();
			try
			{
				var item = new RamStorageActionsAccessor(state, theGenerator);
				current.Value = item;
				try
				{
					action(item);
					InMemoryTransaction.Commit();
					onCommit();
				}
				finally
				{
					current.Value = null;
				}
			}
			finally
			{
				InMemoryTransaction.Dispose();
			}
		}

		public void ExecuteImmediatelyOrRegisterForSyncronization(Action action)
		{
			throw new NotImplementedException();
		}

		public bool Initialize(IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs)
		{
			this.theGenerator = generator;

			Id = Guid.NewGuid();

			return true;
		}

		public void StartBackupOperation(DocumentDatabase database, string backupDestinationDirectory, bool incrementalBackup)
		{
			throw new NotSupportedException();
		}

		public void Restore(string backupLocation, string databaseLocation)
		{
			throw new NotSupportedException();
		}

		public long GetDatabaseSizeInBytes()
		{
			throw new NotSupportedException();
		}

		public string FriendlyName { get; private set; }
		public bool HandleException(Exception exception)
		{
			return exception is DBConcurrencyException;
		}

		public void Compact(InMemoryRavenConfiguration configuration)
		{
			throw new NotSupportedException();
		}

		public Guid ChangeId()
		{
			return Id = Guid.NewGuid();
		}
	}
}
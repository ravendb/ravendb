// -----------------------------------------------------------------------
//  <copyright file="ClusterStateMachine.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading;

using Rachis.Commands;
using Rachis.Interfaces;
using Rachis.Messages;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Impl;
using Raven.Database.Raft.Commands;
using Raven.Database.Raft.Util;
using Raven.Database.Server.Tenancy;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Raft.Storage
{
	public class ClusterStateMachine : IRaftStateMachine
	{
		private readonly PutSerialLock locker = new PutSerialLock();

		private readonly DocumentDatabase database;

		private readonly DatabasesLandlord landlord;

		private long lastAppliedIndex;

		public ClusterStateMachine(DocumentDatabase systemDatabase, DatabasesLandlord databasesLandlord)
		{
			if (systemDatabase == null)
				throw new ArgumentNullException("systemDatabase");

			DatabaseHelper.AssertSystemDatabase(systemDatabase);

			database = systemDatabase;
			landlord = databasesLandlord;

			LastAppliedIndex = ReadLastAppliedIndex();
		}

		private long ReadLastAppliedIndex()
		{
			long result = 0;
			database.TransactionalStorage.Batch(accessor =>
			{
				var item = accessor.Lists.Read("Raven/Cluster", "Status");
				if (item == null)
					return;

				result = item.Data["LastAppliedIndex"].Value<long>();
			});

			return result;
		}

		public void Dispose()
		{
		}

		public long LastAppliedIndex
		{
			get
			{
				return lastAppliedIndex;
			}

			private set
			{
				Thread.VolatileWrite(ref lastAppliedIndex, value);
			}
		}

		public void Apply(LogEntry entry, Command cmd)
		{
			using (locker.Lock())
			{
				database.TransactionalStorage.Batch(accessor =>
				{
					try
					{
						var clusterConfigurationUpdateCommand = cmd as ClusterConfigurationUpdateCommand;
						if (clusterConfigurationUpdateCommand != null)
						{
							Handle(clusterConfigurationUpdateCommand);
							return;
						}

						var databaseUpdateCommand = cmd as DatabaseUpdateCommand;
						if (databaseUpdateCommand != null)
						{
							Handle(databaseUpdateCommand);
							return;
						}

						var databaseDeleteCommand = cmd as DatabaseDeletedCommand;
						if (databaseDeleteCommand != null)
						{
							Handle(databaseDeleteCommand);
							return;
						}
					}
					finally
					{
						UpdateLastAppliedIndex(cmd.AssignedIndex, accessor);
					}
				});
			}
		}

		private void Handle(DatabaseDeletedCommand command)
		{
			var key = DatabaseHelper.GetDatabaseKey(command.Name);

			var documentJson = database.Documents.Get(key, null);
			if (documentJson == null)
				return;

			var document = documentJson.DataAsJson.JsonDeserialization<DatabaseDocument>();
			if (document.IsClusterDatabase() == false)
				return; // ignore non-cluster databases

			var configuration = landlord.CreateTenantConfiguration(DatabaseHelper.GetDatabaseName(command.Name), true);
			if (configuration == null)
				return;

			database.Documents.Delete(key, null, null);

			if (command.HardDelete)
				DatabaseHelper.DeleteDatabaseFiles(configuration);
		}

		private void Handle(DatabaseUpdateCommand command)
		{
			command.Document.AssertClusterDatabase();

			var key = DatabaseHelper.GetDatabaseKey(command.Document.Id);

			var documentJson = database.Documents.Get(key, null);
			if (documentJson != null)
			{
				var document = documentJson.DataAsJson.JsonDeserialization<DatabaseDocument>();
				if (document.IsClusterDatabase() == false)
					return; // TODO [ppekrol] behavior here?
			}

			database.Documents.Put(key, null, RavenJObject.FromObject(command.Document), new RavenJObject(), null);
		}

		private void Handle(ClusterConfigurationUpdateCommand command)
		{
			database.Documents.Put(Constants.Cluster.ClusterConfigurationDocumentKey, null, RavenJObject.FromObject(command.Configuration), new RavenJObject(), null);
		}

		public bool SupportSnapshots
		{
			get
			{
				return false;
			}
		}

		public void CreateSnapshot(long index, long term, ManualResetEventSlim allowFurtherModifications)
		{
			throw new NotImplementedException();
		}

		public ISnapshotWriter GetSnapshotWriter()
		{
			throw new NotImplementedException();
		}

		public void ApplySnapshot(long term, long index, Stream stream)
		{
			throw new NotImplementedException();
		}

		private void UpdateLastAppliedIndex(long index, IStorageActionsAccessor accessor)
		{
			accessor.Lists.Set("Raven/Cluster", "Status", new RavenJObject
			                                              {
				                                              { "LastAppliedIndex", index }
			                                              }, UuidType.DocumentReferences);
			accessor.AfterStorageCommit += () => LastAppliedIndex = index;
		}
	}
}
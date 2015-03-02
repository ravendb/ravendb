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
using Raven.Database.Impl;
using Raven.Database.Raft.Commands;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Raft.Storage
{
	public class ClusterStateMachine : IRaftStateMachine
	{
		private readonly PutSerialLock locker = new PutSerialLock();

		private readonly DocumentDatabase database;

		private long lastAppliedIndex;

		public ClusterStateMachine(DocumentDatabase systemDatabase)
		{
			if (systemDatabase == null)
				throw new ArgumentNullException("systemDatabase");

			if (systemDatabase.Name != null && systemDatabase.Name != Constants.SystemDatabase)
				throw new InvalidOperationException("Must be system database.");

			database = systemDatabase;

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
							HandleClusterConfigurationUpdate(clusterConfigurationUpdateCommand);
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

		private void UpdateLastAppliedIndex(long index, IStorageActionsAccessor accessor)
		{
			accessor.Lists.Set("Raven/Cluster", "Status", new RavenJObject
			                                              {
				                                              { "LastAppliedIndex", index }
			                                              }, UuidType.DocumentReferences);
			accessor.AfterStorageCommit += () => LastAppliedIndex = index;
		}

		private void HandleClusterConfigurationUpdate(ClusterConfigurationUpdateCommand command)
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
	}
}
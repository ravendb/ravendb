// -----------------------------------------------------------------------
//  <copyright file="ClusterStateMachine.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

using Rachis.Commands;
using Rachis.Interfaces;
using Rachis.Messages;

using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Impl;
using Raven.Database.Raft.Commands;
using Raven.Database.Raft.Storage.Handlers;
using Raven.Database.Server.Tenancy;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Raft.Storage
{
	public class ClusterStateMachine : IRaftStateMachine
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		private readonly PutSerialLock locker = new PutSerialLock();

		private readonly DocumentDatabase database;

		private readonly Dictionary<Type, CommandHandler> handlers = new Dictionary<Type, CommandHandler>();

		private long lastAppliedIndex;

		public ClusterStateMachine(DocumentDatabase systemDatabase, DatabasesLandlord databasesLandlord)
		{
			if (systemDatabase == null)
				throw new ArgumentNullException("systemDatabase");

			DatabaseHelper.AssertSystemDatabase(systemDatabase);

			database = systemDatabase;

			LastAppliedIndex = ReadLastAppliedIndex();

			handlers.Add(typeof(ClusterConfigurationUpdateCommand), new ClusterConfigurationUpdateCommandHandler(systemDatabase, databasesLandlord));
			handlers.Add(typeof(DatabaseDeletedCommand), new DatabaseDeletedCommandHandler(systemDatabase, databasesLandlord));
			handlers.Add(typeof(DatabaseUpdateCommand), new DatabaseUpdateCommandHandler(systemDatabase, databasesLandlord));
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
			try
			{
				using (locker.Lock())
				{
					database.TransactionalStorage.Batch(accessor =>
					{
						CommandHandler handler;
						if (handlers.TryGetValue(cmd.GetType(), out handler))
							handler.Handle(cmd);

						UpdateLastAppliedIndex(cmd.AssignedIndex, accessor);
					});
				}
			}
			catch (Exception e)
			{
				log.ErrorException("Could not apply command.", e);
				throw;
			}
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
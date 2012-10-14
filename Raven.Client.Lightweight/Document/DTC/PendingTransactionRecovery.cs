//-----------------------------------------------------------------------
// <copyright file="PendingTransactionRecovery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.IsolatedStorage;
using System.Transactions;
using NLog;
using Raven.Client.Connection;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Document.DTC
{
	public class PendingTransactionRecovery
	{
		private static readonly Logger logger = LogManager.GetCurrentClassLogger();

		public void Execute(IDatabaseCommands commands)
		{
			var resourceManagersRequiringRecovery = new HashSet<Guid>();
			using (var store = IsolatedStorageFile.GetMachineStoreForDomain())
			{
				foreach (var file in store.GetFileNames("*.recovery-information"))
				{
					var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(file);
					Debug.Assert(fileNameWithoutExtension != null);
					var parts = fileNameWithoutExtension.Split(new[] { "-$$-" }, StringSplitOptions.RemoveEmptyEntries);
					if (parts.Length != 2)
						continue;

					Guid resourceManagerId, txId;

					if (Guid.TryParse(parts[0], out resourceManagerId) == false)
						continue;
					if (Guid.TryParse(parts[1], out txId) == false)
						continue;

					try
					{
						using (var fileStream = store.OpenFile(file, FileMode.Open, FileAccess.Read))
						{
							TransactionManager.Reenlist(resourceManagerId, fileStream.ReadData(), new InternalEnlistment(commands, txId));
							resourceManagersRequiringRecovery.Add(resourceManagerId);
							logger.Info("Recovered transaction {0}", txId);
						}
					}
					catch (Exception e)
					{
						logger.WarnException("Could not re-enlist in DTC transaction for tx: " + txId, e);
					}
				}

				foreach (var rm in resourceManagersRequiringRecovery)
				{
					try
					{
						TransactionManager.RecoveryComplete(rm);
					}
					catch (Exception e)
					{
						logger.WarnException("Could not properly complete recovery of resource manager: " + rm, e);
					}
				}
			}
		}

		public class InternalEnlistment : IEnlistmentNotification
		{
			private readonly IDatabaseCommands database;
			private readonly Guid txId;

			public InternalEnlistment(IDatabaseCommands database, Guid txId)
			{
				this.database = database;
				this.txId = txId;
			}

			public void Prepare(PreparingEnlistment preparingEnlistment)
			{
				// shouldn't be called, already 
				// prepared, otherwise we won't have this issue
				preparingEnlistment.Prepared();
			}

			public void Commit(Enlistment enlistment)
			{
				database.Commit(txId);
				enlistment.Done();
			}

			public void Rollback(Enlistment enlistment)
			{
				database.Rollback(txId);
				enlistment.Done();
			}

			public void InDoubt(Enlistment enlistment)
			{
				database.Rollback(txId);
				enlistment.Done();
			}
		}
	}
}

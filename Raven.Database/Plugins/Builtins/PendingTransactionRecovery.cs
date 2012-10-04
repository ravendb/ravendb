//-----------------------------------------------------------------------
// <copyright file="PendingTransactionRecovery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Transactions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Plugins.Builtins
{
	public class PendingTransactionRecovery : IStartupTask
	{
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		public void Execute(DocumentDatabase database)
		{
		    var resourceManagersRequiringRecovery =  new HashSet<Guid>();
			database.TransactionalStorage.Batch(actions =>
			{
				foreach (var txId in actions.Transactions.GetTransactionIds())
				{
					var attachment = actions.Attachments.GetAttachment("transactions/recoveryInformation/" + txId);
					if (attachment == null)//Prepare was not called, there is no recovery information
					{
					    actions.Transactions.RollbackTransaction(txId);
					}
					else
					{
						Guid resourceManagerId;
						if (Guid.TryParse(attachment.Metadata.Value<string>("Resource-Manager-Id"), out resourceManagerId) == false)
					    {
							actions.Transactions.RollbackTransaction(txId);
					    }
					    else
						{
							try
							{
								TransactionManager.Reenlist(resourceManagerId, attachment.Data().ReadData(), new InternalEnlistment(database, txId));
								resourceManagersRequiringRecovery.Add(resourceManagerId);
							}
							catch (Exception e)
							{
								logger.ErrorException("Failed to re-enlist in distributed transaction, transaction has been rolled back", e);
								actions.Transactions.RollbackTransaction(txId);
								actions.Attachments.DeleteAttachment("transactions/recoveryInformation/" + txId, null);
							}
						}
					}
				}
			});
			foreach (var rm in resourceManagersRequiringRecovery)
			{
				TransactionManager.RecoveryComplete(rm);
			}
			
		}

		public class InternalEnlistment : IEnlistmentNotification
		{
			private readonly DocumentDatabase database;
			private readonly Guid txId;

			public InternalEnlistment(DocumentDatabase database, Guid txId)
			{
				this.database = database;
				this.txId = txId;
			}

			public void Prepare(PreparingEnlistment preparingEnlistment)
			{
				byte[] recoveryInformation = preparingEnlistment.RecoveryInformation();
				var ravenJObject = new RavenJObject
				{
					{Constants.NotForReplication, true}
				};
				database.PutStatic("transactions/recoveryInformation/" + txId, null, new MemoryStream(recoveryInformation), ravenJObject);
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

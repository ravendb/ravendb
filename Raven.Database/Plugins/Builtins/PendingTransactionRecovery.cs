using System;
using System.Transactions;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Plugins.Builtins
{
	public class PendingTransactionRecovery : IStartupTask
	{
		public static readonly Guid RavenDbResourceManagerId = Guid.Parse("E749BAA6-6F76-4EEF-A069-40A4378954F8");

		public void Execute(DocumentDatabase database)
		{
			database.TransactionalStorage.Batch(actions =>
			{
				foreach (var txId in actions.General.GetTransactionIds())
				{
					var attachment = actions.Attachments.GetAttachment("transactions/recoveryInformation/" + txId);
					if (attachment == null)//Prepare was not called, there is no recovery information
						actions.General.RollbackTransaction(txId);
					else
						TransactionManager.Reenlist(RavenDbResourceManagerId, attachment.Data, new InternalEnlistment(database, txId));
				}
			});
			
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
				database.PutStatic("transactions/recoveryInformation/" + txId, null, preparingEnlistment.RecoveryInformation(), new JObject());
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
#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="RavenClientEnlistment.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.IO.IsolatedStorage;
using System.Threading;
using System.Transactions;
using Raven.Abstractions.Logging;

namespace Raven.Client.Document
{
	/// <summary>
	/// An implementation of <see cref="IEnlistmentNotification"/> for the Raven Client API, allowing Raven
	/// Client API to participate in Distributed Transactions
	/// </summary>
	public class RavenClientEnlistment : IEnlistmentNotification
	{
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		private readonly ITransactionalDocumentSession session;
		private readonly Action onTxComplete;
		private readonly TransactionInformation transaction;

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenClientEnlistment"/> class.
		/// </summary>
		public RavenClientEnlistment(ITransactionalDocumentSession session, Action onTxComplete)
		{
			transaction = Transaction.Current.TransactionInformation;
			this.session = session;
			this.onTxComplete = onTxComplete;
			TransactionRecoveryInformationFileName = Guid.NewGuid() + ".recovery-information";
		}

		/// <summary>
		/// Notifies an enlisted object that a transaction is being prepared for commitment.
		/// </summary>
		/// <param name="preparingEnlistment">A <see cref="T:System.Transactions.PreparingEnlistment"/> object used to send a response to the transaction manager.</param>
		public void Prepare(PreparingEnlistment preparingEnlistment)
		{
			onTxComplete();
			try
			{
				using (var machineStoreForApplication = IsolatedStorageFile.GetMachineStoreForDomain())
				{
					var name = TransactionRecoveryInformationFileName;
					using (var file = machineStoreForApplication.CreateFile(name + ".temp"))
					using(var writer = new BinaryWriter(file))
					{
						writer.Write(session.ResourceManagerId.ToString());
						writer.Write(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction).ToString());
						writer.Write(session.DatabaseName ?? "");
						writer.Write(preparingEnlistment.RecoveryInformation());
						file.Flush(true);
					}
					machineStoreForApplication.MoveFile(name + ".temp", name);
			}
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not prepare distributed transaction", e);
				preparingEnlistment.ForceRollback(e);
				return;
			}
			preparingEnlistment.Prepared();
		}

		private string TransactionRecoveryInformationFileName { get; set; }

		/// <summary>
		/// Notifies an enlisted object that a transaction is being committed.
		/// </summary>
		/// <param name="enlistment">An <see cref="T:System.Transactions.Enlistment"/> object used to send a response to the transaction manager.</param>
		public void Commit(Enlistment enlistment)
		{
			onTxComplete();
			try
			{
				session.Commit(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));

				DeleteFile();
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not commit distributed transaction", e);
				return; // nothing to do, DTC will mark tx as hang
			}
			enlistment.Done();

		}

		/// <summary>
		/// Notifies an enlisted object that a transaction is being rolled back (aborted).
		/// </summary>
		/// <param name="enlistment">A <see cref="T:System.Transactions.Enlistment"/> object used to send a response to the transaction manager.</param>
		public void Rollback(Enlistment enlistment)
		{
			onTxComplete();
			try
			{
				session.Rollback(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));

				DeleteFile();
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not rollback distributed transaction", e);
			}
			enlistment.Done(); // will happen anyway, tx will be rolled back after timeout
		}

		private void DeleteFile()
		{
			using (var machineStoreForApplication = IsolatedStorageFile.GetMachineStoreForDomain())
			{
				// docs says to retry: http://msdn.microsoft.com/en-us/library/system.io.isolatedstorage.isolatedstoragefile.deletefile%28v=vs.95%29.aspx
				int retries = 10;
				while(true)
				{
					if (machineStoreForApplication.FileExists(TransactionRecoveryInformationFileName) == false)
						break;
					try
					{
						machineStoreForApplication.DeleteFile(TransactionRecoveryInformationFileName);
						break;
					}
					catch (IsolatedStorageException)
					{
						retries -= 1;
						if(retries > 0 )
						{
							Thread.Sleep(100);
							continue;
						}
						throw;
					}
				}
			}
		}

		/// <summary>
		/// Notifies an enlisted object that the status of a transaction is in doubt.
		/// </summary>
		/// <param name="enlistment">An <see cref="T:System.Transactions.Enlistment"/> object used to send a response to the transaction manager.</param>
		public void InDoubt(Enlistment enlistment)
		{
			onTxComplete();
			try
			{
				session.Rollback(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));

				DeleteFile();
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not mark distriubted transaction as in doubt", e);
			}
			enlistment.Done(); // what else can we do?
		}

		/// <summary>
		/// Initializes this instance.
		/// </summary>
		public void Initialize()
		{
		}

		/// <summary>
		/// Rollbacks the specified single phase enlistment.
		/// </summary>
		/// <param name="singlePhaseEnlistment">The single phase enlistment.</param>
		public void Rollback(SinglePhaseEnlistment singlePhaseEnlistment)
		{
			onTxComplete();
			try
			{
				session.Rollback(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));

				DeleteFile();
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not rollback distributed transaction", e);
				singlePhaseEnlistment.InDoubt(e);
				return;
			}
			singlePhaseEnlistment.Aborted();
		}
	}
}
#endif

#if !SILVERLIGHT && !NETFX_CORE
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
using Raven.Client.Document.DTC;

namespace Raven.Client.Document
{
	/// <summary>
	/// An implementation of <see cref="IEnlistmentNotification"/> for the Raven Client API, allowing Raven
	/// Client API to participate in Distributed Transactions
	/// </summary>
	public class RavenClientEnlistment : IEnlistmentNotification
	{
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		private readonly DocumentStoreBase documentStore;
		private readonly ITransactionalDocumentSession session;
		private readonly Action onTxComplete;
		private readonly TransactionInformation transaction;
		private ITransactionRecoveryStorageContext ctx;

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenClientEnlistment"/> class.
		/// </summary>
		public RavenClientEnlistment(DocumentStoreBase documentStore,ITransactionalDocumentSession session, Action onTxComplete)
		{
			transaction = Transaction.Current.TransactionInformation;
			this.documentStore = documentStore;
			this.session = session;
			this.onTxComplete = onTxComplete;
			TransactionRecoveryInformationFileName = Guid.NewGuid() + ".recovery-information";

			ctx = documentStore.TransactionRecoveryStorage.Create();
		}

		/// <summary>
		/// Notifies an enlisted object that a transaction is being prepared for commitment.
		/// </summary>
		/// <param name="preparingEnlistment">A <see cref="T:System.Transactions.PreparingEnlistment"/> object used to send a response to the transaction manager.</param>
		public void Prepare(PreparingEnlistment preparingEnlistment)
		{
			try
			{
				onTxComplete();
				ctx.CreateFile(TransactionRecoveryInformationFileName, stream =>
				{
					var writer = new BinaryWriter(stream);
					writer.Write(session.ResourceManagerId.ToString());
					writer.Write(GetLocalOrDistributedTransactionId(transaction).ToString());
					writer.Write(session.DatabaseName ?? "");
					writer.Write(preparingEnlistment.RecoveryInformation());
				});
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not prepare distributed transaction", e);
			    try
			    {
                    session.Rollback(GetLocalOrDistributedTransactionId(transaction));
                    DeleteFile();
			    }
			    catch (Exception e2)
			    {
			        logger.ErrorException("Could not roll back transaction after prepare failed", e2);
			    }

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
			try
			{
				onTxComplete();
				session.Commit(GetLocalOrDistributedTransactionId(transaction));

				DeleteFile();
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not commit distributed transaction", e);
				return; // nothing to do, DTC will mark tx as hang
			}
			enlistment.Done();
			ctx.Dispose();
		}

		/// <summary>
		/// Notifies an enlisted object that a transaction is being rolled back (aborted).
		/// </summary>
		/// <param name="enlistment">A <see cref="T:System.Transactions.Enlistment"/> object used to send a response to the transaction manager.</param>
		public void Rollback(Enlistment enlistment)
		{
			try
			{
				onTxComplete();
				session.Rollback(GetLocalOrDistributedTransactionId(transaction));

				DeleteFile();
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not rollback distributed transaction", e);
			}
			enlistment.Done(); // will happen anyway, tx will be rolled back after timeout
			ctx.Dispose();
		}

		private void DeleteFile()
		{
			ctx.DeleteFile(TransactionRecoveryInformationFileName);
		}

		/// <summary>
		/// Notifies an enlisted object that the status of a transaction is in doubt.
		/// </summary>
		/// <param name="enlistment">An <see cref="T:System.Transactions.Enlistment"/> object used to send a response to the transaction manager.</param>
		public void InDoubt(Enlistment enlistment)
		{
			try
			{
				onTxComplete();
				session.Rollback(GetLocalOrDistributedTransactionId(transaction));

				DeleteFile();
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not mark distributed transaction as in doubt", e);
			}
			enlistment.Done(); // what else can we do?
			ctx.Dispose();
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
				session.Rollback(GetLocalOrDistributedTransactionId(transaction));

				DeleteFile();
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not rollback distributed transaction", e);
				singlePhaseEnlistment.InDoubt(e);
				return;
			}
			singlePhaseEnlistment.Aborted();
			ctx.Dispose();
		}


		/// <summary>
		/// Gets the local or distributed transaction id.
		/// </summary>
		/// <param name="transactionInformation">The transaction information.</param>
		/// <returns></returns>
		public static Guid GetLocalOrDistributedTransactionId(TransactionInformation transactionInformation)
		{
			if (transactionInformation.DistributedIdentifier != Guid.Empty)
				return transactionInformation.DistributedIdentifier;
			string[] parts = transactionInformation.LocalIdentifier.Split(':');
			if (parts.Length != 2)
				throw new InvalidOperationException("Could not parse TransactionInformation.LocalIdentifier: " + transactionInformation.LocalIdentifier);

			var localOrDistributedTransactionId = new Guid(parts[0]);
			var num = BitConverter.GetBytes(int.Parse(parts[1]));
			byte[] txId = localOrDistributedTransactionId.ToByteArray();
			for (int i = 0; i < num.Length; i++)
			{
				txId[txId.Length - 1 - i] ^= num[i];
			}
			var transactionId = new Guid(txId);
			return transactionId;
		}
	}
}
#endif

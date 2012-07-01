#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="PromotableRavenClientEnlistment.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using System.Transactions;

namespace Raven.Client.Document
{
	/// <summary>
	/// An implementation of <see cref="IPromotableSinglePhaseNotification"/> for RavenDB Client API
	/// </summary>
	public class PromotableRavenClientEnlistment : IPromotableSinglePhaseNotification
	{
		private readonly ITransactionalDocumentSession session;
		private readonly Action onTxComplete;
		private readonly TransactionInformation transaction;

		/// <summary>
		/// Initializes a new instance of the <see cref="PromotableRavenClientEnlistment"/> class.
		/// </summary>
		public PromotableRavenClientEnlistment(ITransactionalDocumentSession session, Action onTxComplete)
		{
			transaction = Transaction.Current.TransactionInformation;
			this.session = session;
			this.onTxComplete = onTxComplete;
		}

		/// <summary>
		/// Notifies an enlisted object that an escalation of the delegated transaction has been requested.
		/// </summary>
		/// <returns>
		/// A transmitter/receiver propagation token that marshals a distributed transaction. For more information, see <see cref="M:System.Transactions.TransactionInterop.GetTransactionFromTransmitterPropagationToken(System.Byte[])"/>.
		/// </returns>
		public byte[] Promote()
		{
			return session.PromoteTransaction(GetLocalOrDistributedTransactionId(transaction));
		}

		/// <summary>
		/// Notifies a transaction participant that enlistment has completed successfully.
		/// </summary>
		/// <exception cref="T:System.Transactions.TransactionException">An attempt to enlist or serialize a transaction.</exception>
		public void Initialize()
		{
		}

		/// <summary>
		/// Notifies an enlisted object that the transaction is being committed.
		/// </summary>
		/// <param name="singlePhaseEnlistment">A <see cref="T:System.Transactions.SinglePhaseEnlistment"/> interface used to send a response to the transaction manager.</param>
		public void SinglePhaseCommit(SinglePhaseEnlistment singlePhaseEnlistment)
		{
			onTxComplete();

			session.Commit(GetLocalOrDistributedTransactionId(transaction));
			singlePhaseEnlistment.Committed();
		}

		/// <summary>
		/// Notifies an enlisted object that the transaction is being rolled back.
		/// </summary>
		/// <param name="singlePhaseEnlistment">A <see cref="T:System.Transactions.SinglePhaseEnlistment"/> object used to send a response to the transaction manager.</param>
		public void Rollback(SinglePhaseEnlistment singlePhaseEnlistment)
		{
			onTxComplete(); 
			session.Rollback(GetLocalOrDistributedTransactionId(transaction));
			singlePhaseEnlistment.Aborted();
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
			if(parts.Length != 2)
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

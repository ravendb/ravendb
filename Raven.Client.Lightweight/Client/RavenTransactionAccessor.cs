//-----------------------------------------------------------------------
// <copyright file="RavenTransactionAccessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using System.Transactions;
using Raven.Client.Document;
using Raven.Database.Data;
using TransactionInformation = Raven.Http.TransactionInformation;

namespace Raven.Client.Client
{
	/// <summary>
	/// Provide access to the current transaction 
	/// </summary>
	public static class RavenTransactionAccessor
	{
#if !NET_3_5
		private static readonly ThreadLocal<Stack<TransactionInformation>> currentRavenTransactions = new ThreadLocal<Stack<TransactionInformation>>(() => new Stack<TransactionInformation>());

		/// <summary>
		/// Starts a transaction
		/// </summary>
		/// <returns></returns>
		public static IDisposable StartTransaction()
		{
			return StartTransaction(TimeSpan.FromMinutes(1));
		}

		/// <summary>
		/// Starts a transaction with the specified timeout
		/// </summary>
		/// <param name="timeout">The timeout.</param>
		/// <returns></returns>
		public static IDisposable StartTransaction(TimeSpan timeout)
		{
			currentRavenTransactions.Value.Push(new TransactionInformation
			{
				Id = Guid.NewGuid(),
				Timeout = timeout
			});
			return new DisposableAction(() => currentRavenTransactions.Value.Pop());
		}
#endif
		/// <summary>
		/// Gets the transaction information for the current transaction
		/// </summary>
		/// <returns></returns>
		public static TransactionInformation GetTransactionInformation()
		{
			#if !NET_3_5
			if (currentRavenTransactions.Value.Count >0)
				return currentRavenTransactions.Value.Peek();
			#endif
			if (Transaction.Current == null)
				return null;
			return new TransactionInformation
			{
				Id = PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(Transaction.Current.TransactionInformation),
				Timeout = TransactionManager.DefaultTimeout
			};
		}
	}
}

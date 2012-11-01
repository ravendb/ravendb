//-----------------------------------------------------------------------
// <copyright file="RavenTransactionAccessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !SILVERLIGHT
using System;
using System.Collections.Generic;
using System.Transactions;
using Raven.Abstractions.Extensions;
using Raven.Client.Document;
using TransactionInformation = Raven.Abstractions.Data.TransactionInformation;

namespace Raven.Client.Connection
{
	/// <summary>
	/// Provide access to the current transaction 
	/// </summary>
	public static class RavenTransactionAccessor
	{
		[ThreadStatic]
		private static Stack<TransactionInformation> currentRavenTransactions;

		private static Stack<TransactionInformation> CurrentRavenTransactions
		{
			get
			{
				if(currentRavenTransactions == null)
					currentRavenTransactions =  new Stack<TransactionInformation>();
				return currentRavenTransactions;
			}
		}

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
			CurrentRavenTransactions.Push(new TransactionInformation
			{
				Id = Guid.NewGuid(),
				Timeout = timeout
			});
			return new DisposableAction(() => CurrentRavenTransactions.Pop());
		}
		
		/// <summary>
		/// Gets the transaction information for the current transaction
		/// </summary>
		/// <returns></returns>
		public static TransactionInformation GetTransactionInformation()
		{
			if (CurrentRavenTransactions.Count >0)
				return CurrentRavenTransactions.Peek();
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
#endif
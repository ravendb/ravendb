//-----------------------------------------------------------------------
// <copyright file="RavenTransactionAccessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Transactions;
using Raven.Abstractions.Extensions;
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

		[ThreadStatic]
		private static bool supressExplicitRavenTransaction = false;

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
				Id = Guid.NewGuid().ToString(),
				Timeout = timeout
			});
			return new DisposableAction(() => CurrentRavenTransactions.Pop());
		}
		
		public static TimeSpan? DefaultTimeout { get; set; }


		internal static IDisposable SupressExplicitRavenTransaction()
		{
			supressExplicitRavenTransaction = true;
			return new DisposableAction(() =>
			{
				supressExplicitRavenTransaction = false;
			});
		}
	}
}
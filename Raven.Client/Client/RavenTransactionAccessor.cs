using System;
using System.Collections.Generic;
using System.Threading;
using System.Transactions;
using Raven.Client.Document;
using Raven.Database.Data;
using TransactionInformation = Raven.Database.TransactionInformation;

namespace Raven.Client.Client
{
	public static class RavenTransactionAccessor
	{
#if !NET_3_5
		private static readonly ThreadLocal<Stack<TransactionInformation>> currentRavenTransactions = new ThreadLocal<Stack<TransactionInformation>>(() => new Stack<TransactionInformation>());

		public static IDisposable StartTransaction()
		{
			return StartTransaction(TimeSpan.FromMinutes(1));
		}

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
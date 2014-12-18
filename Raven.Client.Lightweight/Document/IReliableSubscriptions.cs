using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Client.Document
{
	public interface IReliableSubscriptions : IDisposable
	{
		long Create(SubscriptionCriteria criteria, string database = null);
		Subscription Open(long id, SubscriptionConnectionOptions options, string database = null);
		List<SubscriptionDocument> GetSubscriptions(int start, int take, string database = null);
		void Delete(long id, string database = null);
		void Release(long id, string database = null);
	}
}
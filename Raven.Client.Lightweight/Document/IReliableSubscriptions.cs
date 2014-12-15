using System;
using Raven.Abstractions.Data;

namespace Raven.Client.Document
{
	public interface IReliableSubscriptions : IDisposable
	{
		long Create(SubscriptionCriteria criteria, string database = null);
		Subscription Open(long id, SubscriptionBatchOptions options, string database = null);
	}
}
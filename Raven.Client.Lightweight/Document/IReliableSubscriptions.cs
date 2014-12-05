using Raven.Abstractions.Data;

namespace Raven.Client.Document
{
	public interface IReliableSubscriptions
	{
		Subscription Create(string name, SubscriptionCriteria criteria, SubscriptionBatchOptions options, string database = null);
	}
}
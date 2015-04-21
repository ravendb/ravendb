using Raven.Client.Connection;
using Raven.Client.Counters.Actions;

namespace Raven.Client.Counters
{
	public interface ICountersReplicationInformer : IReplicationInformerBase<CountersClient>
	{
	}
}

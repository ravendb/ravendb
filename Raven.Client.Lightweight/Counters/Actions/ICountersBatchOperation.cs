using System;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;

namespace Raven.Client.Counters.Actions
{
	public interface ICountersBatchOperation : IDisposable
	{
		void ScheduleChange(string groupName, string counterName, long delta);
		
		void ScheduleIncrement(string groupName, string counterName);
		
		void ScheduleDecrement(string groupName, string counterName);

		Guid OperationId { get; }

		CountersBatchOptions Options { get; }

		Task FlushAsync();
	}
}
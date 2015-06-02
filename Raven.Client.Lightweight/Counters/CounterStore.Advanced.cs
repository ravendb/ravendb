using System;
using Raven.Abstractions.Counters;
using Raven.Client.Counters.Actions;
using Raven.Client.Counters.Operations;

namespace Raven.Client.Counters
{
    public partial class CounterStore
    {
		public class CounterStoreAdvancedOperations
		{
			private readonly CounterStore parent;

			internal CounterStoreAdvancedOperations(CounterStore parent)
			{
				this.parent = parent;
			}

			public CountersBatchOperation NewBatch(CountersBatchOptions options = null)
			{
				if (parent.Name == null)
					throw new ArgumentException("Default Counter Storage isn't set!");

				return new CountersBatchOperation(parent, parent.Name, options);
			}

			public CountersBatchOperation NewBatch(string counterStorageName, CountersBatchOptions options = null)
			{
				if (string.IsNullOrWhiteSpace(counterStorageName))
					throw new ArgumentException("Counter Storage name isn't set!");

				return new CountersBatchOperation(parent, counterStorageName, options);
			}
		}
    }
}

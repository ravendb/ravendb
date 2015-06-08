using System;
using Raven.Abstractions.Counters;
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
					throw new ArgumentException("Counter Storage isn't set!");

				parent.AssertInitialized();

				return new CountersBatchOperation(parent, parent.Name, options);
			}
		}
    }
}

using System;
using Raven.Abstractions.TimeSeries;
using Raven.Client.TimeSeries.Operations;

namespace Raven.Client.TimeSeries
{
    public partial class TimeSeriesStore
    {
		public class TimeSeriesStoreAdvancedOperations
		{
			private readonly TimeSeriesStore parent;

			internal TimeSeriesStoreAdvancedOperations(TimeSeriesStore parent)
			{
				this.parent = parent;
			}

			public TimeSeriesBatchOperation NewBatch(TimeSeriesBatchOptions options = null)
			{
				if (parent.Name == null)
					throw new ArgumentException("Time series isn't set!");

				parent.AssertInitialized();

				return new TimeSeriesBatchOperation(parent, parent.Name, options);
			}
		}
    }
}

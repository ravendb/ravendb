using System.Collections.Generic;
using System.Linq;

namespace Raven.Database.Indexing
{
    public class ReduceBatchSizeAutoTuner : BaseBatchSizeAutoTuner
    {
        public ReduceBatchSizeAutoTuner(WorkContext context)
            : base(context)
        {
            LastAmountOfItemsToRemember = 1;
            InstallGauges();
        }

        private void InstallGauges()
        {
            var metricCounters = context.MetricsCounters;
            metricCounters.AddGauge(typeof(ReduceBatchSizeAutoTuner), "InitialNumberOfItems", () => InitialNumberOfItems);
            metricCounters.AddGauge(typeof(ReduceBatchSizeAutoTuner), "MaxNumberOfItems", () => MaxNumberOfItems);
            metricCounters.AddGauge(typeof(ReduceBatchSizeAutoTuner), "CurrentNumberOfItems", () => CurrentNumberOfItems);
        }

        public override int InitialNumberOfItems
        {
            get { return context.Configuration.InitialNumberOfItemsToReduceInSingleBatch; }
        }

        protected override int MaxNumberOfItems
        {
            get { return context.Configuration.MaxNumberOfItemsToReduceInSingleBatch; }
        }

        protected override int CurrentNumberOfItems
        {
            get { return context.CurrentNumberOfItemsToReduceInSingleBatch; }
            set { context.CurrentNumberOfItemsToReduceInSingleBatch = value; }
        }

        protected override sealed int LastAmountOfItemsToRemember { get; set; }

        private List<int> lastAmountOfItemsToReduce = new List<int>();

        protected override void RecordAmountOfItems(int numberOfItems)
        {
            var currentLastAmountOfItemsToReduce = lastAmountOfItemsToReduce;

            var amountToTake = currentLastAmountOfItemsToReduce.Count;

            if (amountToTake + 1 >= LastAmountOfItemsToRemember)
                amountToTake = currentLastAmountOfItemsToReduce.Count - 1;

            lastAmountOfItemsToReduce = new List<int>(currentLastAmountOfItemsToReduce.Take(amountToTake))
                                        {
                                            numberOfItems
                                        };
        }

        protected override IEnumerable<int> GetLastAmountOfItems()
        {
            return lastAmountOfItemsToReduce;
        }

        protected override string Name
        {
            get { return "ReduceBatchSizeAutoTuner"; }
        }
    }
}

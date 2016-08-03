using System;
using System.Linq;
using System.Collections.Generic;
using System.Reactive.Disposables;

namespace Raven.Database.Indexing
{
    public class IndexBatchSizeAutoTuner : BaseBatchSizeAutoTuner
    {
        public IndexBatchSizeAutoTuner(WorkContext context)
            : base(context)
        {
            LastAmountOfItemsToRemember = 1;
            InstallGauges();
        }

        private void InstallGauges()
        {
            var metricCounters = context.MetricsCounters;
            metricCounters.AddGauge(typeof(IndexBatchSizeAutoTuner), "InitialNumberOfItems", () => InitialNumberOfItems);
            metricCounters.AddGauge(typeof(IndexBatchSizeAutoTuner), "MaxNumberOfItems", () => MaxNumberOfItems);
            metricCounters.AddGauge(typeof(IndexBatchSizeAutoTuner), "CurrentNumberOfItems", () => CurrentNumberOfItems);
        }

        public override int InitialNumberOfItems
        {
            get { return context.Configuration.InitialNumberOfItemsToProcessInSingleBatch; }
        }

        protected override int MaxNumberOfItems
        {
            get { return context.Configuration.MaxNumberOfItemsToProcessInSingleBatch; }
        }

        protected override int CurrentNumberOfItems
        {
            get { return context.CurrentNumberOfItemsToIndexInSingleBatch; }
            set { context.CurrentNumberOfItemsToIndexInSingleBatch = value; }
        }
        
        protected override sealed int LastAmountOfItemsToRemember { get; set; }

        private List<int> lastAmountOfItemsToIndex = new List<int>();

        protected override void RecordAmountOfItems(int numberOfItems)
        {
            var currentLastAmountOfItemsToIndex = lastAmountOfItemsToIndex;

            var amountToTake = currentLastAmountOfItemsToIndex.Count;
            
            if (amountToTake + 1 >= LastAmountOfItemsToRemember)
                amountToTake = currentLastAmountOfItemsToIndex.Count - 1;

            lastAmountOfItemsToIndex = new List<int>(currentLastAmountOfItemsToIndex.Take(amountToTake))
                                        {
                                            numberOfItems
                                        };
        }

        protected override IEnumerable<int> GetLastAmountOfItems()
        {
            return lastAmountOfItemsToIndex;
        }

        protected override string Name
        {
            get { return "IndexBatchSizeAutoTuner"; }
        }

        public IDisposable ConsiderLimitingNumberOfItemsToProcessForThisBatch(int? maxIndexOutputsPerDoc, bool containsMapReduceIndexes)
        {
            if (maxIndexOutputsPerDoc == null || maxIndexOutputsPerDoc <= (containsMapReduceIndexes ? context.Configuration.MaxMapReduceIndexOutputsPerDocument : context.Configuration.MaxSimpleIndexOutputsPerDocument))
                return null;

            var oldValue = NumberOfItemsToProcessInSingleBatch;

            int indexOutputsPerDocLog = (int) Math.Log(maxIndexOutputsPerDoc.Value);
            indexOutputsPerDocLog = indexOutputsPerDocLog < 1 ? 1 : indexOutputsPerDocLog;
            var newValue = Math.Max(NumberOfItemsToProcessInSingleBatch / indexOutputsPerDocLog, InitialNumberOfItems);

            if (oldValue == newValue)
                return null;

            NumberOfItemsToProcessInSingleBatch = newValue;
            return Disposable.Create(() => NumberOfItemsToProcessInSingleBatch = oldValue);
        }
    }
}

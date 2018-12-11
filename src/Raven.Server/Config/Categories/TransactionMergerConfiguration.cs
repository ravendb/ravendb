using System;
using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Config.Categories
{
    public class TransactionMergerConfiguration : ConfigurationCategory
    {
        public TransactionMergerConfiguration(bool forceUsing32BitsPager)
        {
            if (IntPtr.Size == sizeof(int) || forceUsing32BitsPager)
            {
                MaxTxSize = new Size(4, SizeUnit.Megabytes);
                return;
            }

            var memoryInfo = MemoryInformation.GetMemoryInfo();

            MaxTxSize = Size.Min(
                new Size(512, SizeUnit.Megabytes),
                memoryInfo.TotalPhysicalMemory / 10);
        }

        [Description("EXPERT: Time to wait after the previous async commit is completed before checking for the tx size")]
        [DefaultValue(0)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("TransactionMerger.MaxTimeToWaitForPreviousTxInMs", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MaxTimeToWaitForPreviousTx { get; set; }

        [Description("EXPERT: Time to wait for the previous async commit transaction before rejecting the request due to long duration IO")]
        [DefaultValue(5000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("TransactionMerger.MaxTimeToWaitForPreviousTxBeforeRejectingInMs", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MaxTimeToWaitForPreviousTxBeforeRejecting { get; set; }

        [Description("EXPERT: Maximum size for the merged transaction")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("TransactionMerger.MaxTxSizeInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size MaxTxSize { get; set; }
    }
}

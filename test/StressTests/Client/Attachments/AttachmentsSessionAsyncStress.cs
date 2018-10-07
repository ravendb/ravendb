using System;
using System.Threading.Tasks;
using FastTests;
using SlowTests.Client.Attachments;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Client.Attachments
{
    public class AttachmentsSessionAsyncStress : NoDisposalNeeded
    {
        [NightlyBuildTheory]
        [InlineData(100_000)]
        [InlineData(1_000_000)]
        public async Task PutLotOfAttachments(int count)
        {
            if (IntPtr.Size == sizeof(int))
                count = 100_000;
            using (var stress = new AttachmentsSessionAsync())
            {
                await stress.PutLotOfAttachments(count);
            }
        }
    }
}
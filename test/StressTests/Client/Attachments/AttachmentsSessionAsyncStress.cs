using System.Threading.Tasks;
using FastTests;
using SlowTests.Client.Attachments;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Client.Attachments
{
    public class AttachmentsSessionAsyncStress : NoDisposalNeeded
    {
        [NightlyBuildTheory64]
        [InlineData(100_000)]
        [InlineData(1_000_000)]
        public async Task PutLotOfAttachments(int count)
        {
            using (var stress = new AttachmentsSessionAsync())
            {
                await stress.PutLotOfAttachments(count);
            }
        }

        [NightlyBuildTheory32]
        [InlineData(50_000)]
        public async Task PutLotOfAttachments32(int count)
        {
            using (var stress = new AttachmentsSessionAsync())
            {
                await stress.PutLotOfAttachments(count);
            }
        }
    }
}

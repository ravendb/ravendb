using System.Threading.Tasks;
using FastTests.Client.Attachments;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Client.Attachments
{
    public class AttachmentsSessionAsyncStress
    {
        [NightlyBuildTheory]
        [InlineData(100_000)]
        [InlineData(1_000_000)]
        public async Task PutLotOfAttachments(int count)
        {
            using (var stress = new AttachmentsSessionAsync())
            {
                await stress.PutLotOfAttachments(count);
            }
        }
    }
}
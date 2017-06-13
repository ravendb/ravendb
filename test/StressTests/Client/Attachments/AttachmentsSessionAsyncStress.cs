using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client.Attachments;
using Xunit;

namespace StressTests.Client.Attachments
{
    public class AttachmentsSessionAsyncStress
    {
        [Theory]
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
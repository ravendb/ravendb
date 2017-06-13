using FastTests.Client.Attachments;
using Xunit;

namespace StressTests.Client.Attachments
{
    public class AttachmentsSessionStress
    {
        [Theory]
        [InlineData(100_000)]
        [InlineData(1_000_000)]
        public void PutLotOfAttachments(int count)
        {
            using (var stress = new AttachmentsSession())
            {
                stress.PutLotOfAttachments(count);
            }
        }
    }
}
using FastTests.Client.Attachments;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Client.Attachments
{
    public class AttachmentsSessionStress
    {
        [Fact]
        public void PutLotOfAttachments()
        {
            using (var stress = new AttachmentsSession())
            {
                stress.PutLotOfAttachments(1000);
            }
        }

        [NightlyBuildTheory]
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
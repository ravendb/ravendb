using FastTests;
using FastTests.Client.Attachments;
using Microsoft.AspNetCore.Http.Features;
using Xunit;

namespace StressTests.Client.Attachments
{
    public class AttachmentsHugeFiles : NoDisposalNeeded
    {
        [Theory]
        [InlineData(FormOptions.DefaultMultipartBodyLengthLimit * 2, "vEbE0Uh02lIPx/cEFBagkmepLTP0nWWYX5+exkt9yoE=")] // 256 MB
        [InlineData(2.5 * 1024 * 1024 * 1024, "2ssXqJM7lbdDpDNkc2GsfDbmcQ6CXdgP6/LFmLtFCT4=")] // 2.5 GB
        public void BatchRequestWithLongMultiPartSections(long size, string hash)
        {
            using (var stress = new AttachmentsBigFiles())
            {
                stress.BatchRequestWithLongMultiPartSections(size, hash);
            }
        }

        [Theory]
        [InlineData(2.5 * 1024 * 1024 * 1024, "2ssXqJM7lbdDpDNkc2GsfDbmcQ6CXdgP6/LFmLtFCT4=")] // 2.5 GB
        public void SupportHugeAttachment(long size, string hash)
        {
            using (var stress = new AttachmentsBigFiles())
            {
                stress.SupportHugeAttachment(size, hash);
            }
        }
    }
}
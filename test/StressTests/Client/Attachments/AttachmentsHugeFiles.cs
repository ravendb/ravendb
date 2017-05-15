using FastTests;
using FastTests.Client.Attachments;
using Microsoft.AspNetCore.Http.Features;
using Xunit;

namespace StressTests.Client.Attachments
{
    public class AttachmentsHugeFiles : NoDisposalNeeded
    {
        [Theory]
        [InlineData(FormOptions.DefaultMultipartBodyLengthLimit * 2, "G/VBSDnFqmLKAphJbokRdiXpfeRMcTwz")] // 256 MB
        public void AttachmentBiggerThan128Mb_WhichIsMaxMultipartBodyLengthLimit(long size, string hash)
        {
            using (var stress = new AttachmentsBigFiles())
            {
                stress.AttachmentBiggerThan128Mb_WhichIsMaxMultipartBodyLengthLimit(size, hash);
            }
        }

        [Theory]
        [InlineData(2.5 * 1024 * 1024 * 1024, "gxtSDE78gM6tU9lmqq2GIRgYOXiy6BKh")] // 2.5 GB
        public void SupportHugeAttachment(long size, string hash)
        {
            using (var stress = new AttachmentsBigFiles())
            {
                stress.SupportHugeAttachment(size, hash);
            }
        }
    }
}
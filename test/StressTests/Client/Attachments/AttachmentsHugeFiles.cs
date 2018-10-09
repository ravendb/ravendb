using System.IO;
using System.Threading.Tasks;
using FastTests;
using Microsoft.AspNetCore.Http.Features;
using SlowTests.Client.Attachments;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Client.Attachments
{
    public class AttachmentsHugeFiles : NoDisposalNeeded
    {
        [NightlyBuildTheory64Bit]
        [InlineData(FormOptions.DefaultMultipartBodyLengthLimit * 2, "vEbE0Uh02lIPx/cEFBagkmepLTP0nWWYX5+exkt9yoE=")] // 256 MB
        [InlineData(2.5 * 1024 * 1024 * 1024, "2ssXqJM7lbdDpDNkc2GsfDbmcQ6CXdgP6/LFmLtFCT4=")] // 2.5 GB
        public async Task BatchRequestWithLongMultiPartSections(long size, string hash)
        {
            try
            {
                using (var stress = new AttachmentsBigFiles())
                {
                    await stress.BatchRequestWithLongMultiPartSections(size, hash);
                }
            }
            catch (IOException ioe)
            {
                if (ioe.Message.Contains("Stream was too long"))
                    throw new IOException("Not enough memory to run this test. Consider running stress tests one by one", ioe);
                throw;
            }
        }

        [NightlyBuildTheory64Bit]
        [InlineData(2.5 * 1024 * 1024 * 1024, "2ssXqJM7lbdDpDNkc2GsfDbmcQ6CXdgP6/LFmLtFCT4=")] // 2.5 GB
        public async Task SupportHugeAttachment(long size, string hash)
        {
            try
            {
                using (var stress = new AttachmentsBigFiles())
                {
                    await stress.SupportHugeAttachment(size, hash);
                }
            }
            catch (IOException ioe)
            {
                if (ioe.Message.Contains("Stream was too long"))
                    throw new IOException("Not enough memory to run this test. Consider running stress tests one by one", ioe);
                throw;
            }
        }
    }
}

using System.Threading.Tasks;
using SlowTests.Client.Attachments;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Client.Attachments
{
    public class BulkInsertAttachmentsStress : NoDisposalNoOutputNeeded
    {
        public BulkInsertAttachmentsStress(ITestOutputHelper output) : base(output)
        {
        }

        [MultiplatformTheory(RavenArchitecture.AllX64)]
        [InlineData(100, 32 * 1024 * 1024)]
        public async Task StoreManyAttachmentsStress(int count, int size)
        {
            using (var test = new BulkInsertAttachments(Output))
            {
                await test.StoreManyAttachments(count, size);
            }
        }

        [MultiplatformTheory(RavenArchitecture.AllX64)]
        [InlineData(1000, 100, 32 * 1024)]
        [InlineData(1000, 100, 64 * 1024)]
        public async Task StoreManyAttachmentsAndDocsStress(int count, int attachments, int size)
        {
            using (var test = new BulkInsertAttachments(Output))
            {
                await test.StoreManyAttachmentsAndDocs(count, attachments, size);
            }
        }
    }
}

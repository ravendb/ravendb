using System.Threading.Tasks;
using SlowTests.Client.Attachments;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Client.Attachments
{
    public class AttachmentsStreamStressFromSlow : NoDisposalNoOutputNeeded
    {
        public AttachmentsStreamStressFromSlow(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformTheory(RavenTestCategory.Attachments, RavenArchitecture.AllX64)]
        [InlineData(128 * 1024 * 1024)]
        [InlineData(1024 * 1024 * 1024)]
        public async Task CanGetOneAttachment(int size)
        {
            await using (var test = new AttachmentsStreamTests(Output))
            {
                test.CanGetOneAttachment(size);
            }
        }

        [RavenMultiplatformTheory(RavenTestCategory.Attachments, RavenArchitecture.AllX64)]
        [InlineData(128 * 1024 * 1024)]
        public async Task CanGetOneAttachmentAsync(int size)
        {
            await using (var test = new AttachmentsStreamTests(Output))
            {
                await test.CanGetOneAttachmentAsync(size);
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(1_000, 32768)]
        [InlineData(10_000, 1)]
        public async Task CanGetListOfAttachmentsAndSkip(int count, int size)
        {
            await using (var test = new AttachmentsStreamTests(Output))
            {
                test.CanGetListOfAttachmentsAndSkip(count, size);
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(1_000, 32768)]
        [InlineData(10_000, 1)]
        public async Task CanGetListOfAttachmentsAndReadOrdered(int count, int size)
        {
            await using (var test = new AttachmentsStreamTests(Output))
            {
                test.CanGetListOfAttachmentsAndReadOrdered(count, size);
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(1_000, 32768)]
        [InlineData(10_000, 1)]
        public async Task CanGetListOfAttachmentsAndReadOrderedAsync(int count, int size)
        {
            await using (var test = new AttachmentsStreamTests(Output))
            {
                await test.CanGetListOfAttachmentsAndReadOrderedAsync(count, size);
            }
        }
    }
}

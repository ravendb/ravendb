using System.Threading.Tasks;
using SlowTests.Client.Attachments;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Client.Attachments
{
    public class AttachmentsSessionStress : NoDisposalNoOutputNeeded
    {
        public AttachmentsSessionStress(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(1000)]
        [InlineData(10_000)]
        public async Task PutLotOfAttachments(int count)
        {
            await using (var stress = new AttachmentsSession(Output))
            {
                stress.PutLotOfAttachments(count);
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(1000)]
        [InlineData(10_000)]
        public async Task PutLotOfAttachmentsAsync(int count)
        {
            await using (var stress = new AttachmentsSessionAsync(Output))
            {
                await stress.PutLotOfAttachments(count);
            }
        }

        [NightlyBuildMultiplatformTheory(RavenArchitecture.AllX64)]
        [InlineData(10_000)]
        [InlineData(100_000)]
        public async Task StressPutLotOfAttachments(int count)
        {
            await using (var stress = new AttachmentsSession(Output))
            {
                stress.PutLotOfAttachments(count);
            }
        }

        [NightlyBuildMultiplatformTheory(RavenArchitecture.AllX86)]
        [InlineData(10_000)]
        public async Task StressPutLotOfAttachments32(int count)
        {
            await using (var stress = new AttachmentsSession(Output))
            {
                stress.PutLotOfAttachments(count);
            }
        }

        [NightlyBuildMultiplatformTheory(RavenArchitecture.AllX64)]
        [InlineData(10_000)]
        [InlineData(100_000)]
        public async Task StressPutLotOfAttachmentsAsync(int count)
        {
            await using (var stress = new AttachmentsSessionAsync(Output))
            {
                await stress.PutLotOfAttachments(count);
            }
        }

        [NightlyBuildMultiplatformTheory(RavenArchitecture.AllX86)]
        [InlineData(10_000)]
        public async Task StressPutLotOfAttachmentsAsync32(int count)
        {
            await using (var stress = new AttachmentsSessionAsync(Output))
            {
                await stress.PutLotOfAttachments(count);
            }
        }
    }
}

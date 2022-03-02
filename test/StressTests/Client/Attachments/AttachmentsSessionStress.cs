using System.Threading.Tasks;
using FastTests;
using SlowTests.Client.Attachments;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Client.Attachments
{
    public class AttachmentsSessionStress : NoDisposalNoOutputNeeded
    {
        public AttachmentsSessionStress(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(1000)]
        [InlineData(10_000)]
        public void PutLotOfAttachments(int count)
        {
            using (var stress = new AttachmentsSession(Output))
            {
                stress.PutLotOfAttachments(count);
            }
        }

        [Theory]
        [InlineData(1000)]
        [InlineData(10_000)]
        public async Task PutLotOfAttachmentsAsync(int count)
        {
            using (var stress = new AttachmentsSessionAsync(Output))
            {
                await stress.PutLotOfAttachments(count);
            }
        }

        [NightlyBuildMultiplatformTheory(RavenArchitecture.AllX64)]
        [InlineData(10_000)]
        [InlineData(100_000)]
        public void StressPutLotOfAttachments(int count)
        {
            using (var stress = new AttachmentsSession(Output))
            {
                stress.PutLotOfAttachments(count);
            }
        }

        [NightlyBuildMultiplatformTheory(RavenArchitecture.AllX86)]
        [InlineData(10_000)]
        public void StressPutLotOfAttachments32(int count)
        {
            using (var stress = new AttachmentsSession(Output))
            {
                stress.PutLotOfAttachments(count);
            }
        }

        [NightlyBuildMultiplatformTheory(RavenArchitecture.AllX64)]
        [InlineData(10_000)]
        [InlineData(100_000)]
        public async Task StressPutLotOfAttachmentsAsync(int count)
        {
            using (var stress = new AttachmentsSessionAsync(Output))
            {
                await stress.PutLotOfAttachments(count);
            }
        }

        [NightlyBuildMultiplatformTheory(RavenArchitecture.AllX86)]
        [InlineData(10_000)]
        public async Task StressPutLotOfAttachmentsAsync32(int count)
        {
            using (var stress = new AttachmentsSessionAsync(Output))
            {
                await stress.PutLotOfAttachments(count);
            }
        }
    }
}

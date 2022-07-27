using System.Threading.Tasks;
using FastTests;
using SlowTests.Client.Attachments;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Client.Attachments
{
    public class AttachmentsSessionAsyncStress : NoDisposalNoOutputNeeded
    {
        public AttachmentsSessionAsyncStress(ITestOutputHelper output) : base(output)
        {
        }

        [NightlyBuildMultiplatformTheory(RavenArchitecture.AllX64)]
        [InlineData(100_000)]
        [InlineData(1_000_000)]
        public async Task PutLotOfAttachments(int count)
        {
            using (var stress = new AttachmentsSessionAsync(Output))
            {
                await stress.PutLotOfAttachments(count);
            }
        }

        [NightlyBuildMultiplatformTheory(RavenArchitecture.AllX86)]
        [InlineData(50_000)]
        public async Task PutLotOfAttachments32(int count)
        {
            using (var stress = new AttachmentsSessionAsync(Output))
            {
                await stress.PutLotOfAttachments(count);
            }
        }
    }
}

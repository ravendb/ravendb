using System.Runtime.CompilerServices;

namespace Tests.Infrastructure
{
    public class NightlyBuildAzureStorageEmulatorFact : AzureStorageEmulatorFact
    {
        public NightlyBuildAzureStorageEmulatorFact([CallerMemberName] string memberName = "")
            : base(memberName)
        {
            if (NightlyBuildTheoryAttribute.IsNightlyBuild == false)
                Skip = NightlyBuildTheoryAttribute.SkipMessage;
        }
    }
}

using System.Runtime.CompilerServices;

namespace Tests.Infrastructure
{
    public class NightlyBuildAzureFact : AzureFactAttribute
    {
        public NightlyBuildAzureFact([CallerMemberName] string memberName = "")
            : base(memberName)
        {
            if (NightlyBuildTheoryAttribute.IsNightlyBuild == false)
                Skip = NightlyBuildTheoryAttribute.SkipMessage;
        }
    }
}

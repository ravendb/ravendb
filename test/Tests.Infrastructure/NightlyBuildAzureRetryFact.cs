using System.Runtime.CompilerServices;

namespace Tests.Infrastructure
{
    public class NightlyBuildAzureRetryFact : AzureRetryFactAttribute
    {
        public NightlyBuildAzureRetryFact([CallerMemberName] string memberName = "")
            : base(memberName)
        {
            if (NightlyBuildTheoryAttribute.IsNightlyBuild == false)
                Skip = NightlyBuildTheoryAttribute.SkipMessage;
        }
    }
}

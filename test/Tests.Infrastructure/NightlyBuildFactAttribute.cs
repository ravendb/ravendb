using Xunit;

namespace Tests.Infrastructure
{
    public class NightlyBuildFactAttribute : FactAttribute
    {
        public override string Skip
        {
            get
            {
                if (NightlyBuildTheoryAttribute.IsNightlyBuild)
                    return null;

                return NightlyBuildTheoryAttribute.SkipMessage;
            }
        }
    }
}

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

        public static bool ShouldSkip(bool nightlyBuildRequired, out string skip)
        {
            skip = null;
            
            if (nightlyBuildRequired == false)
                return false;
            
            if (NightlyBuildTheoryAttribute.IsNightlyBuild)
                return false;

            skip = NightlyBuildTheoryAttribute.SkipMessage;
            return true;
        }
    }
}

using Xunit;

namespace Tests.Infrastructure
{
    public class NightlyBuildFactAttribute : FactAttribute, Xunit.v3.IFactAttribute
    {
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

        public new string Skip
        {
            get
            {
                if (NightlyBuildTheoryAttribute.IsNightlyBuild)
                    return null;

                return NightlyBuildTheoryAttribute.SkipMessage;
            }
        }

        public static bool ShouldSkip(out string skipMessage)
        {
            if (NightlyBuildTheoryAttribute.IsNightlyBuild)
            {
                skipMessage = null;
                return false;
            }

            skipMessage = NightlyBuildTheoryAttribute.SkipMessage;
            return true;
        }
    }
}

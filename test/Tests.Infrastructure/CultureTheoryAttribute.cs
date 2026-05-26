using Xunit;

namespace Tests.Infrastructure
{
    public class CultureTheoryAttribute : TheoryAttribute, Xunit.v3.IFactAttribute
    {
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

        public new string Skip
        {
            get
            {
                if (string.IsNullOrEmpty(base.Skip) == false)
                    return base.Skip;

                if (RavenTestHelper.EnvironmentVariables.EnableCultureTests == false)
                    return $"Culture tests are disabled. Please set '{RavenTestHelper.EnvironmentVariables.EnableCultureTestsEnvName}' environment variable to 'true' to enable them.";

                return null;
            }
            set => base.Skip = value;
        }
    }
}

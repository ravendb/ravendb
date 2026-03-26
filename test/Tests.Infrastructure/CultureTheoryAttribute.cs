using System;
using Xunit;

namespace Tests.Infrastructure
{
    public class CultureTheoryAttribute : TheoryAttribute, Xunit.v3.IFactAttribute
    {
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

        private readonly bool _enable;

        public CultureTheoryAttribute()
        {
            var variable = Environment.GetEnvironmentVariable("RAVEN_ENABLE_CULTURE_TESTS");
            if (variable == null || bool.TryParse(variable, out _enable) == false)
                _enable = false;
        }

        public new string Skip
        {
            get
            {
                if (_enable == false)
                    return "Culture tests are disabled. Please set 'RAVEN_ENABLE_CULTURE_TESTS' environment variable to 'true' to enable them.";

                return null;
            }
        }
    }
}
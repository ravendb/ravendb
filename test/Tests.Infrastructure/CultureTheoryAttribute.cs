using System;
using Xunit;

namespace Tests.Infrastructure
{
    public class CultureTheoryAttribute : TheoryAttribute
    {
        private readonly bool _skip;

        public CultureTheoryAttribute()
        {
            var variable = Environment.GetEnvironmentVariable("RAVEN_ENABLE_CULTURE_TESTS");
            if (variable == null || bool.TryParse(variable, out _skip) == false)
                _skip = true;
        }

        public override string Skip
        {
            get
            {
                if (_skip)
                    return "Culture tests are disabled. Please set 'RAVEN_ENABLE_CULTURE_TESTS' environment variable to 'true' to enable them.";

                return null;
            }
        }
    }
}
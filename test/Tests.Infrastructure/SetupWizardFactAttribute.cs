using System;
using Raven.Client.Documents.Linq;
using Xunit;

namespace Tests.Infrastructure
{
    public class SetupWizardFactAttribute : FactAttribute
    {
        private readonly bool _enable;

        public SetupWizardFactAttribute()
        {
            var variable = Environment.GetEnvironmentVariable("RAVEN.License.Path");

            if (variable == null || (Environment.MachineName.Contains("scratch") == false && Environment.MachineName.Contains("ubuntu") == false))
            {
                //Skip = "Test can run only on CI scratch machines and if a license path was provided in environment variable RAVEN.License.Path";
            }
        }
    }
}

using Raven.Database.Plugins;

namespace Raven.Tests.Linq
{
    public class SampleDynamicCompilationExtension : AbstractDynamicCompilationExtension
    {
        public override string[] GetNamespacesToImport()
        {
            return new[]
            {
                typeof (SampleGeoLocation).Namespace
            };
        }

        public override string[] GetAssembliesToReference()
        {
            return new[]
            {
                typeof (SampleGeoLocation).Assembly.Location
            };
        }
    }
}
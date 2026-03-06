using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;
using Xunit.Sdk;
using Xunit.v3;

namespace Tests.Infrastructure
{
    public class CriticalCulturesAttribute : DataAttribute
    {
        public override bool SupportsDiscoveryEnumeration() => false;

        public static CultureInfo InvariantCulture = CultureInfo.InvariantCulture;
        
        private static readonly CultureInfo[] Cultures =
        {
            CultureInfo.InvariantCulture,
            CultureInfo.CurrentCulture,
            new CultureInfo("NL"), // Uses comma instead of point: 12,34
            new CultureInfo("tr-TR") // "The Turkey Test"
        };

        public override ValueTask<IReadOnlyCollection<ITheoryDataRow>> GetData(MethodInfo testMethod, DisposalTracker disposalTracker)
        {
            var result = Cultures.Select(c => (ITheoryDataRow)new TheoryDataRow(new object[] { c })).ToArray();
            return new ValueTask<IReadOnlyCollection<ITheoryDataRow>>(result);
        }
    }
}

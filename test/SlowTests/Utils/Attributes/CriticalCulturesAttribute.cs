using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using Xunit.Sdk;

namespace SlowTests.Utils.Attributes
{
    public class CriticalCulturesAttribute : DataAttribute
    {
        private static readonly CultureInfo[] Cultures =
        {
            CultureInfo.InvariantCulture,
            CultureInfo.CurrentCulture,
            new CultureInfo("NL"), // Uses comma instead of point: 12,34
            new CultureInfo("tr-TR") // "The Turkey Test"
        };

        public override IEnumerable<object[]> GetData(MethodInfo testMethod)
        {
            return Cultures.Select(c => new object[] { c });
        }
    }
}
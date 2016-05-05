
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Sdk;

namespace FastTests
{
    [XunitTestCaseDiscoverer("FastTests.SkippableFactDiscoverer", "FastTests")]
    public class SkippableFactAttribute : FactAttribute { }
}

using FastTests.Voron;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26110(ITestOutputHelper output) : StorageTest(output)
{
}

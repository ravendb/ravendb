using System;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class StackAllocInTest : RavenTestBase
{
    public StackAllocInTest(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void GetStackoverflow()
    {
       Span<byte> SO = stackalloc byte[1024 * 1024 * 1024];
       IfNotGetSOYet(0);
    }

    private int IfNotGetSOYet(int value)
    {
        return IfNotGetSOYet(value);
    }
}

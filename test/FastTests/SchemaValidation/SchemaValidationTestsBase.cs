using System.Runtime.CompilerServices;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.SchemaValidation;

public abstract class SchemaValidationTestsBase : ParallelTestBase
{
    private readonly JsonOperationContext _context;

    protected SchemaValidationTestsBase(ITestOutputHelper output, [CallerFilePath] string sourceFile = "") : base(output, sourceFile)
    {
        _context = JsonOperationContext.ShortTermSingleUse();
    }
    
    protected void AssertError(string expected, string actual)
    {
        Assert.True(expected.StartsWith(actual), $"expected: '{expected}', actual: '{actual}'.");
    }
    
    protected BlittableJsonReaderObject ReadObject(DynamicJsonValue obj) => _context.ReadObject(obj,"test object");
    
    public override void Dispose()
    {
        _context?.Dispose();
        base.Dispose();
    }
}

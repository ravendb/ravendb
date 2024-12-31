using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.SchemaValidation;

public abstract class SchemaValidationTestsBase : ParallelTestBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
    protected SchemaValidationTestsBase(ITestOutputHelper output, [CallerFilePath] string sourceFile = "") : base(output, sourceFile)
    {
    }
    
    protected static void AssertError(string expected, string actual)
    {
        Assert.True(expected.StartsWith(actual), $"expected: '{expected}', actual: '{actual}'.");
    }

    protected static async Task AssertMultipleParallel(params Action[] checks)
    {
        await Task.WhenAll(checks.Select(Task.Run));
    }
    
    protected static JsonOperationContext ReadObjectOnNewCtx(DynamicJsonValue obj, out BlittableJsonReaderObject readObj) 
    {
        var context = JsonOperationContext.ShortTermSingleUse();

        readObj = context.ReadObject(obj, "test object");
        return context;
    }

    protected static string Regex([StringSyntax(StringSyntaxAttribute.Regex)] string pattern) => pattern;
}

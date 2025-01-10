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
    protected JsonContextPool ContextPool { get; }

    // ReSharper disable once ConvertToPrimaryConstructor
    protected SchemaValidationTestsBase(ITestOutputHelper output, [CallerFilePath] string sourceFile = "") : base(output, sourceFile)
    {
        ContextPool = new JsonContextPool();
    }


    protected static void AssertError(string expected, string actual)
    {
        Assert.True(actual.StartsWith(expected), $"expected: '{expected}', \nactual: '{actual}'.");
    }

    protected static async Task AssertMultipleParallel(params Action[] checks)
    {
        var tasks = checks.Select(Task.Run).ToArray();
        await Task.WhenAll(tasks.Select(x => x.ContinueWith(_ => { })));
        var whenAll = Task.WhenAll(tasks);
        if (whenAll.IsFaulted)
            throw whenAll.Exception;
    }
    
    protected static JsonOperationContext ReadObjectOnNewCtx(DynamicJsonValue obj, out BlittableJsonReaderObject readObj) 
    {
        var context = JsonOperationContext.ShortTermSingleUse();

        readObj = context.ReadObject(obj, "test object");
        return context;
    }

    protected static string Regex([StringSyntax(StringSyntaxAttribute.Regex)] string pattern) => pattern;

    public override void Dispose()
    {
        ContextPool.Dispose();
        base.Dispose();
    }
}

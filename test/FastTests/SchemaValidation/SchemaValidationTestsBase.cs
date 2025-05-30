using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Server.SchemaValidation;
using Raven.Server.SchemaValidation.ErrorMessage;
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
}

public static class SchemaValidationTestsHelper
{
    public static bool Validate(this SchemaValidator validator, BlittableJsonReaderObject obj, out string errors)
    {
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (var errorBuilder = new ErrorBuilder(context))
        {
            if (validator.Validate(obj, errorBuilder))
            {
                errors = null;
                return true;
            }

            errors = new string(errorBuilder.GetErrors());
            return false;
        }
    }
}

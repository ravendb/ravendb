using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Server.Documents.SchemaValidation;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.SchemaValidation;

public abstract class SchemaValidationTestsBase : ParallelTestBase
{
    // ReSharper disable once ConvertToPrimaryConstructor
    protected SchemaValidationTestsBase(ITestOutputHelper output, [CallerFilePath] string sourceFile = "") : base(output, sourceFile)
    {
    }

    protected static SchemaValidatorSettings SchemaValidatorSettings { get;  } = new SchemaValidatorSettings{RegexTimeout = TimeSpan.FromSeconds(1), MaxDepth = 64};
    
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
    
    public static JsonOperationContext ReadObjectOnNewCtx(DynamicJsonValue obj, out BlittableJsonReaderObject readObj) 
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

            errors = errorBuilder.ToString();
            return false;
        }
    }
}

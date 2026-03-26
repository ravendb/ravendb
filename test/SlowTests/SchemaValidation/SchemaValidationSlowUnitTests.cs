using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.SchemaValidation;
using Raven.Server.Documents.SchemaValidation;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using SVC = Raven.Server.Documents.SchemaValidation.SchemaValidatorConstants;

namespace SlowTests.SchemaValidation;

public class SchemaValidationSlowUnitTests : SchemaValidationTestsBase
{
    public SchemaValidationSlowUnitTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateObjectConstantWithMassiveConcurrency()
    {
        const int concurrency = 100;
        
        using var context = JsonOperationContext.ShortTermSingleUse();

        var schemaValidator = new SchemaValidator();
        var jsonValue = new DynamicJsonValue();
        Fill(jsonValue, 5);
        
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["objectProp"] = new DynamicJsonValue { [SVC.Const] = jsonValue }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        using var barrier = new Barrier(concurrency);
        var tasks = Enumerable.Range(0, concurrency).Select(_ =>
        {
            var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["objectProp"] = jsonValue }, out var obj);
            
            return Task.Run(() =>
            {
                using var _ = ctx;
                barrier.SignalAndWait();
                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            });
        }).ToArray();
        await Task.WhenAll(tasks);
    }

    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateObjectEnumWithMassiveConcurrency()
    {
        const int concurrency = 100;
        
        using var context = JsonOperationContext.ShortTermSingleUse();

        var schemaValidator = new SchemaValidator();
        var jsonValue = new DynamicJsonValue();
        Fill(jsonValue, 5);
        
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Type] = "object",
            [SVC.Properties] = new DynamicJsonValue
            {
                ["objectProp"] = new DynamicJsonValue { [SVC.Enum] = new DynamicJsonArray{jsonValue} }
            }
        };
        using var _ = ReadObjectOnNewCtx(schemaDefinition, out var blitSchemaDefinition);
        schemaValidator.Init(blitSchemaDefinition, SchemaValidatorSettings);

        using var barrier = new Barrier(concurrency);
        var tasks = Enumerable.Range(0, concurrency).Select(_ =>
        {
            var ctx = ReadObjectOnNewCtx(new DynamicJsonValue { ["objectProp"] = jsonValue }, out var obj);
            
            return Task.Run(() =>
            {
                using var _ = ctx;
                barrier.SignalAndWait();
                Assert.True(schemaValidator.Validate(obj, out var errors), errors);
            });
        }).ToArray();
        await Task.WhenAll(tasks);
    }

    private static void Fill(DynamicJsonValue parent, int depth)
    {
        parent["strprop"] = "somevalue";
        
        for (var i = 0; i < depth; i++)
        {
            var obj = new DynamicJsonValue();
            Fill(obj, depth-1);
            parent[$"prop{i}"] = obj;
        }
    }
    
    [RavenFact(RavenTestCategory.JavaScript)]
    public async Task SchemaValidation_WhenValidateEnumWithCompressedStringWithMassiveConcurrency()
    {
        const int concurrency = 100;
        var usageMode = BlittableJsonDocumentBuilder.UsageMode.CompressSmallStrings;
        
        var schemaValidator = new SchemaValidator();
        var value = string.Join("", Enumerable.Repeat("there", 4));
        
        var schemaDefinition = new DynamicJsonValue
        {
            [SVC.Properties] = new DynamicJsonValue
            {
                ["strProp"] = new DynamicJsonValue { [SVC.Enum] = new DynamicJsonArray{value} }
            }
        };
        
        using var schemaCtx = JsonOperationContext.ShortTermSingleUse();
        using var readObj = schemaCtx.ReadObject(schemaDefinition, "test object", usageMode);
        
        schemaValidator.Init(readObj, SchemaValidatorSettings);

        using var barrier = new Barrier(concurrency);
        var tasks = Enumerable.Range(0, concurrency).Select(_ => Task.Run(() =>
        {
            using var ctx = JsonOperationContext.ShortTermSingleUse();
            var obj = ctx.ReadObject(new DynamicJsonValue { ["strProp"] = value }, "test object");
            barrier.SignalAndWait();
            Assert.True(schemaValidator.Validate(obj, out var errors), errors);
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
}

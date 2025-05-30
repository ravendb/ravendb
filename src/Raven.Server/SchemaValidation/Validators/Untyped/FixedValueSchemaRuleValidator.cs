using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Untyped;

public abstract class FixedValueSchemaRuleValidator : SchemaRuleValidator<object>
{
    protected static object ConvertTypeForComparison(object v)
    {
        return v switch
        {
            LazyNumberValue lnv => (decimal)lnv,
            long lv => (decimal)lv,
            decimal 
                or LazyStringValue 
                or LazyCompressedStringValue 
                or BlittableJsonReaderObject 
                or BlittableJsonReaderArray 
                or bool 
                or null => v,
            _ => throw new InvalidOperationException($"The type {v.GetType()} is not supported.")
        };
    }

    protected static bool IsString(object constantValue)
    {
        return SchemaValidationHelper.GetPublicTypeOfObj(constantValue) == SchemaValidationHelper.String;
    }
}

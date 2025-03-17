using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators;

public abstract class FixedValueSchemaRuleValidator : SchemaRuleValidator<object>
{
    protected static object ConvertTypeForComparison(object v)
    {
        return v switch
        {
            LazyNumberValue lnv => (decimal)lnv,
            long lv => (decimal)lv,
            decimal => v,
            LazyStringValue  => v,
            LazyCompressedStringValue lcsv => lcsv.ToLazyStringValue(),
            BlittableJsonReaderObject or BlittableJsonReaderArray => v,
            _ => throw new InvalidOperationException($"The type {v.GetType()} is not supported.")
        };
    }
}

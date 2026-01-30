using System;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Untyped;

public abstract class FixedValueSchemaRuleValidator : SchemaRuleValidator<object>
{
    protected static object ConvertTypeForComparison(object v, bool cloneAsRoot)
    {
        return v switch
        {
            LazyNumberValue lnv => (decimal)lnv,
            long lv => (decimal)lv,
            // TODO: The Clone can be removed after RavenDB-25633 is fixed
            BlittableJsonReaderObject obj => cloneAsRoot ? obj.CloneOnTheSameContext() : obj,
            BlittableJsonReaderArray array => cloneAsRoot ? array.Clone() : array,
            LazyStringValue or LazyCompressedStringValue or decimal or bool or null => v,
            _ => throw new InvalidOperationException($"The type {v.GetType()} is not supported.")
        };
    }


    protected static bool SafeConcurrentEquals(JsonOperationContext operationContext, object schemaValue, object documentValue)
    {
        if (schemaValue is BlittableJsonReaderObject schemaObj)
            schemaValue = schemaObj.CloneForConcurrentRead(operationContext);
        
        if (schemaValue is BlittableJsonReaderArray schemaArray)
            schemaValue = schemaArray.CloneForConcurrentRead(operationContext);
        
        if (schemaValue is LazyStringValue schemaLsv)
            schemaValue =  schemaLsv.CloneForConcurrentRead(operationContext);
        
        if (schemaValue is LazyCompressedStringValue schemaLcsv)
            // schemaValue = schemaLcsv;
            schemaValue = schemaLcsv.CloneForConcurrentRead(operationContext);

        return Equals(schemaValue, documentValue);
    }

    protected static bool IsString(object constantValue)
    {
        return SchemaValidationHelper.GetPublicTypeOfObj(constantValue) == SchemaValidationHelper.String;
    }
}

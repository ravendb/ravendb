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
            LazyStringValue or LazyCompressedStringValue => v.ToString(),
            BlittableJsonReaderObject obj => cloneAsRoot ? obj.CloneOnTheSameContext() : obj,
            BlittableJsonReaderArray array => cloneAsRoot ? array.Clone() : array,
            decimal or bool or null => v,
            _ => throw new InvalidOperationException($"The type {v.GetType()} is not supported.")
        };
    }


    protected static bool SafeConcurrentEquals(object schemaValue, object documentValue)
    {
        if (schemaValue is BlittableJsonReaderObject schemaObj)
        {
            if (documentValue is not BlittableJsonReaderObject documentObj)
                return false;
            return schemaObj.CloneForConcurrentRead(documentObj._context).Equals(documentObj);
        }
        
        if (schemaValue is BlittableJsonReaderArray schemaArray)
        {
            if (documentValue is not BlittableJsonReaderArray documentArray)
                return false;
            return schemaArray.CloneForConcurrentRead(documentArray._context).Equals(documentArray);
        }

        return Equals(schemaValue, documentValue);
    }

    protected static bool IsString(object constantValue)
    {
        return SchemaValidationHelper.GetPublicTypeOfObj(constantValue) == SchemaValidationHelper.String;
    }
}

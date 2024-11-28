using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Object;

public class ConstantSchemaRuleValidator : SchemaRuleValidator<object>
{
    public const string RuleName = "const";
    
    private readonly object _constantValue;

    public static ConstantSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition)
    {
        if (schemaDefinition.TryGet(RuleName, out object @const) == false)
            //TODO Should not happen. Also maybe collect all error to return full error report
            return null;

        return new ConstantSchemaRuleValidator(@const);
    }
    
    private ConstantSchemaRuleValidator(object constantValue)
    {
        _constantValue = ConvertType(constantValue);
    }

    //TODO Consider defining base class with EnumSchemaRuleValidator
    private object ConvertType(object x)
    {
        if (x is LazyNumberValue lnx)
        {
            return (decimal)lnx;
        }

        if (x is LazyStringValue or LazyCompressedStringValue)
        {
            return x.ToString();
        }

        if (x is BlittableJsonReaderObject or BlittableJsonReaderArray)
        {
            //TODO To have context in the validator and clone the blittables and maybe also LazyStringValue and LazyNumberValue
            return x.ToString();
        }

        if (x is long lx)
        {
            return (decimal)lx;
        }

        //Should not happen
        throw new InvalidOperationException($"The type {x.GetType()} is not supported.");
    }
    
    protected override void ValidateInternal(object value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if(_constantValue.Equals(value) == false)
            //TODO Clear error to differentiate between number and string (15 or "15")
            errorBuilder.AddError($"The value at '{path}' must be '{_constantValue}', but it is '{value}'.");
    }

    protected override bool CheckTypeAndGetValue(object value, out object tValue)
    {
        tValue = ConvertType(value);
        return true;
    }
}

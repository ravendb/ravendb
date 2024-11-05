using System;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Object;

[SchemaRule("const")]
public class ConstantSchemaRuleValidator : SchemaRuleValidator<object>
{
    private readonly object _constantValue;

    public ConstantSchemaRuleValidator(string path, object constantValue) : base(path)
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

        throw new InvalidOperationException($"The type {x.GetType()} is not supported. {Path}");
    }
    
    protected override void ValidateInternal(object value, IErrorBuilder errorBuilder)
    {
        if(_constantValue.Equals(value) == false)
            //TODO Clear error to differentiate between number and string (15 or "15")
            errorBuilder.AddError($"The value at '{Path}' must be '{_constantValue}', but it is '{value}'.");
    }

    protected override bool CheckTypeAndGetValue(object value, out object tValue)
    {
        tValue = ConvertType(value);
        return true;
    }
}

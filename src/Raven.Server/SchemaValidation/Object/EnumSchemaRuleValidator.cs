using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Object;

public class EnumSchemaRuleValidator : SchemaRuleValidator<object>
{
    public const string RuleName = "enum";

    private readonly object[] _enums;

    public static EnumSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition)
    {
        if (schemaDefinition.TryGet(RuleName, out BlittableJsonReaderArray @enum) == false)
            //TODO Should not happen. Also maybe collect all error to return full error report on read
            return null;
        
        return new EnumSchemaRuleValidator(@enum);
    }
    
    private EnumSchemaRuleValidator(IEnumerable<object> enums)
    {
        _enums = enums.Select(ConvertType).ToArray();
    }

    //TODO Consider defining base class with ConstantSchemaRuleValidator
    private object ConvertType(object x)
    {
        if (x is LazyNumberValue lnx)
            return (decimal)lnx;

        if (x is LazyStringValue or LazyCompressedStringValue)
            return x.ToString();

        if (x is BlittableJsonReaderObject or BlittableJsonReaderArray)
            //TODO To have context in the validator and clone the blittables and maybe also LazyStringValue and LazyNumberValue
            return x.ToString();

        if (x is long lx)
            return (decimal)lx;
        
        if (x is decimal)
            return x;

        throw new InvalidOperationException($"The type {x.GetType()} is not supported.");
    }

    protected override void ValidateInternal(object value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if(_enums.Any(x => x.Equals(value)) == false)
            //TODO Clear error to differentiate between number and string (15 or "15")
            errorBuilder.AddError($"The value '{value}' at '{path}' is not an allowed value. Expected one of: {string.Join(", ", _enums)}.");
    }
    
    protected override bool CheckTypeAndGetValue(object value, out object tValue)
    {
        tValue = ConvertType(value);
        return true;
    }
}

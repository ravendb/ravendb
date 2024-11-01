using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Object;

[SchemaRule("enum")]
public class EnumSchemaRuleValidator : SchemaRuleValidator<object>
{
    private readonly object[] _enum;

    public EnumSchemaRuleValidator(string path, IEnumerable<object> @enum) : base(path)
    {
        _enum = @enum.Select(ConvertType).ToArray();
    }

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
        if(_enum.Any(x => x.Equals(ConvertType(value))) == false)
            //TODO Clear error to differentiate between number and string (15 or "15")
            errorBuilder.AddError($"The value '{value}' at '{Path}' is not an allowed value. Expected one of: {string.Join(", ", _enum)}.");
    }
}

using System;

namespace Raven.Server.SchemaValidation.Number;

[SchemaRule("multipleOf")]
public class MultipleOfSchemaRuleValidator : NumberSchemaRuleValidator
{
    private readonly decimal _multipleOf;

    public MultipleOfSchemaRuleValidator(string path, decimal multipleOf) : base(path)
    {
        _multipleOf = multipleOf;
    }

    protected override void ValidateInternal(decimal value, IErrorBuilder errorBuilder)
    {
        if(value % _multipleOf != 0)
            errorBuilder.AddError($"The value '{value}' at '{Path}' should be a multiple of {_multipleOf}.");
    }
}

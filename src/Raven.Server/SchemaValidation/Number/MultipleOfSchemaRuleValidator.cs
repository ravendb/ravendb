using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Number;

public class MultipleOfSchemaRuleValidator : NumberSchemaRuleValidator
{
    public const string RuleName = "multipleOf";

    private readonly decimal _multipleOf;

    public static MultipleOfSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition)
    {
        if (schemaDefinition.TryGet(RuleName, out decimal multipleOf) == false)
            //TODO Should not happen. Also maybe collect all error to return full error report
            return null;

        return new MultipleOfSchemaRuleValidator(multipleOf);
    }
    
    private MultipleOfSchemaRuleValidator(decimal multipleOf)
    {
        _multipleOf = multipleOf;
    }

    protected override void ValidateInternal(decimal value, SchemaValidatorPath path, IErrorBuilder errorBuilder)
    {
        if(value % _multipleOf != 0)
            errorBuilder.AddError($"The value '{value}' at '{path}' should be a multiple of {_multipleOf}.");
    }
}

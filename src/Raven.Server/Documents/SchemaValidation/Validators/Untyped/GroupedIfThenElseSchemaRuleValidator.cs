using Raven.Server.Documents.SchemaValidation.ErrorMessage;

namespace Raven.Server.Documents.SchemaValidation.Validators.Untyped;

public class GroupedIfThenElseSchemaRuleValidator : SchemaRuleValidator<object>
{
    private readonly IfThenElseSchemaRuleValidator[] _ifThenElseSchemaRuleValidators;

    // ReSharper disable once ConvertToPrimaryConstructor
    public GroupedIfThenElseSchemaRuleValidator(IfThenElseSchemaRuleValidator[] ifThenElseSchemaRuleValidators)
    {
        _ifThenElseSchemaRuleValidators = ifThenElseSchemaRuleValidators;
    }

    public override bool Validate(SchemaValidationContext context, object value)
    {
        var isValid = true;
        foreach (var dependentRequire in _ifThenElseSchemaRuleValidators)
        {
            isValid = dependentRequire.Validate(context, value);
            if (isValid == false && context.ErrorBuilder == null)
                return false;
        }

        return isValid;
    }
}

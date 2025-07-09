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

    public override bool Validate(object value, ErrorBuilder errorBuilder)
    {
        var isValid = true;
        foreach (var dependentRequire in _ifThenElseSchemaRuleValidators)
        {
            isValid = dependentRequire.Validate(value, errorBuilder);
            if (isValid == false && errorBuilder == null)
                return false;
        }

        return isValid;
    }
}

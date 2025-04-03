using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

public class GroupedIfThenElseSchemaRuleValidator : SchemaRuleValidator<BlittableJsonReaderObject>
{
    private readonly IfThenElseSchemaRuleValidator[] _ifThenElseSchemaRuleValidators;

    // ReSharper disable once ConvertToPrimaryConstructor
    public GroupedIfThenElseSchemaRuleValidator(IfThenElseSchemaRuleValidator[] ifThenElseSchemaRuleValidators)
    {
        _ifThenElseSchemaRuleValidators = ifThenElseSchemaRuleValidators;
    }

    public override bool Validate(BlittableJsonReaderObject value, ErrorBuilder errorBuilder)
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

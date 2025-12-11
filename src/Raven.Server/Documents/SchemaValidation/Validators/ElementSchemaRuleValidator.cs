using System;
using System.Diagnostics;
using System.Linq;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators;

[DebuggerDisplay("'{SchemaPath}' property validator")]
public class ElementSchemaRuleValidator
{
    // ReSharper disable once PrivateFieldCanBeConvertedToLocalVariable
    private readonly ISchemaRuleValidator[] _ruleValidators;
    private readonly Type[] _typesRestriction;
    private readonly string[] _publicTypesRestriction;
    protected readonly SchemaPath SchemaPath;
    
    public BlittableJsonReaderObject SchemaDefinition { get; init; }

    // ReSharper disable once ConvertToPrimaryConstructor
    public ElementSchemaRuleValidator(Type[] typesRestriction, ISchemaRuleValidator[] ruleValidators, SchemaPath schemaPath)
        : this(ruleValidators, schemaPath)
    {
        _typesRestriction = typesRestriction;
        _publicTypesRestriction = _typesRestriction?.Select(SchemaValidationHelper.GetPublicType).Distinct().ToArray();
    }
    
    public ElementSchemaRuleValidator(ISchemaRuleValidator[] ruleValidators, SchemaPath schemaPath)
    {
        _ruleValidators = ruleValidators;
        SchemaPath = schemaPath;
    }
    
    public bool Validate(object value, ErrorBuilder errorBuilder)
    {
        if (IsOfRequiredType(value) == false)
        {
            errorBuilder?.AddError($"'{errorBuilder.Path}' should be of type '{_publicTypesRestriction:' or '}' but actual type is '{SchemaValidationHelper.GetPublicType(value?.GetType())}'.");
            return false;
        }
        
        return CheckAllValidators(value, errorBuilder);
    }

    private bool CheckAllValidators(object value, ErrorBuilder errorBuilder)
    {
        if (_ruleValidators == null)
            return true;

        var isValid = true;
        foreach (var ruleValidator in _ruleValidators)
        {
            isValid &= ruleValidator.Validate(value, errorBuilder);
            if (errorBuilder == null && isValid == false)
                return false;
        }
        return isValid;
    }

    private bool IsOfRequiredType(object obj) => _typesRestriction == null || _typesRestriction.Length == 0 || _typesRestriction.Contains(obj?.GetType());
}

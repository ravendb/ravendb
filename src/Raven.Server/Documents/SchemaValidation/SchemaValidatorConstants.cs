namespace Raven.Server.Documents.SchemaValidation;

internal static class SchemaValidatorConstants
{
    public const string Type = "type";
    public const string Description = "description";
    public const string Const = "const";
    public const string Enum = "enum";
    public const string Ref = "$ref";
    public const string Defs = "$defs";

    #region Numbers
    public const string Maximum = "maximum";
    public const string ExclusiveMaximum = "exclusiveMaximum";
    public const string Minimum = "minimum";
    public const string ExclusiveMinimum = "exclusiveMinimum";
    public const string MultipleOf = "multipleOf";
    #endregion
    
    #region String
    public const string MaxLength = "maxLength";
    public const string MinLength = "minLength";
    public const string Pattern = "pattern";
    #endregion

    #region Object
    public const string Properties = "properties";
    public const string PatternProperties = "patternProperties";
    public const string AdditionalProperties = "additionalProperties";
    public const string MaxProperties = "maxProperties";
    public const string MinProperties = "minProperties";
    public const string PropertyNames = "propertyNames";
    public const string Required = "required";
    public const string ExcludedProperties = "x-excludedProperties";
    #endregion
    
    #region Array
    public const string UniqueItems = "uniqueItems";
    public const string Items = "items";
    public const string PrefixItems = "prefixItems";
    public const string Contains = "contains";
    public const string MinContains = "minContains";
    public const string MaxContains = "maxContains";
    #endregion

    #region conditional
    public const string DependentRequired = "dependentRequired";
    public const string DependentSchemas = "dependentSchemas";
    public const string If = "if";
    public const string Then = "then";
    public const string Else = "else";
    public const string Not = "not";
    public const string AllOf = "allOf";
    public const string OneOf = "oneOf";
    public const string AnyOf = "anyOf";
    #endregion
}

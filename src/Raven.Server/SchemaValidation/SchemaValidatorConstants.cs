namespace Raven.Server.SchemaValidation;

internal static class SchemaValidatorConstants
{
    // ReSharper disable InconsistentNaming
    public const string type = "type";
    public const string description = "description";
    public const string @const = "const";
    public const string @enum = "enum";

    #region Numbers
    public const string maximum = "maximum";
    public const string exclusiveMaximum = "exclusiveMaximum";
    public const string minimum = "minimum";
    public const string exclusiveMinimum = "exclusiveMinimum";
    public const string multipleOf = "multipleOf";
    #endregion
    
    #region String
    public const string maxLength = "maxLength";
    public const string minLength = "minLength";
    public const string pattern = "pattern";
    #endregion

    #region Object
    public const string properties = "properties";
    public const string patternProperties = "patternProperties";
    public const string additionalProperties = "additionalProperties";
    public const string maxProperties = "maxProperties";
    public const string minProperties = "minProperties";
    public const string propertyNames = "propertyNames";
    public const string required = "required";
    #endregion
    
    #region Array
    public const string uniqueItems = "uniqueItems";
    public const string items = "items";
    public const string prefixItems = "prefixItems";
    public const string contains = "contains";
    #endregion
    // ReSharper restore InconsistentNaming
}

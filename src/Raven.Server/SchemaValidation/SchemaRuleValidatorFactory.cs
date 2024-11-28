using System;
using System.Collections.Generic;
using Raven.Server.SchemaValidation.Number;
using Raven.Server.SchemaValidation.Object;
using Raven.Server.SchemaValidation.String;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation;

public static class SchemaRuleValidatorFactory
{
    private static readonly Dictionary<string, Func<BlittableJsonReaderObject, SchemaRuleValidator>> SchemaRuleValidatorFactories= new Dictionary<string, Func<BlittableJsonReaderObject, SchemaRuleValidator>>
    {
        #region numbers
        {MaximumSchemaRuleValidator.RuleName, MaximumSchemaRuleValidator.Create},
        {MinimumSchemaRuleValidator.RuleName, MinimumSchemaRuleValidator.Create},
        {MultipleOfSchemaRuleValidator.RuleName, MultipleOfSchemaRuleValidator.Create},
        #endregion
        
        #region objects
        {ConstantSchemaRuleValidator.RuleName, ConstantSchemaRuleValidator.Create},
        {EnumSchemaRuleValidator.RuleName, EnumSchemaRuleValidator.Create},
        #endregion
        
        #region strings
        {MaximumLengthSchemaRuleValidator.RuleName, MaximumLengthSchemaRuleValidator.Create},
        {MinimumLengthSchemaRuleValidator.RuleName, MinimumLengthSchemaRuleValidator.Create},
        {RegexLengthSchemaRuleValidator.RuleName, RegexLengthSchemaRuleValidator.Create},
        #endregion
    };
    
    public static bool TryCreateValidator(string rule, BlittableJsonReaderObject schemaDefinition, out SchemaRuleValidator validator)
    {
        if (SchemaRuleValidatorFactories.TryGetValue(rule, out Func<BlittableJsonReaderObject, SchemaRuleValidator> factory))
        {
            validator = factory(schemaDefinition);
            return true;
        }
        validator = null;
        return false;
    }
    
}

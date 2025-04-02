using System;
using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

[SchemaRule(SchemaValidatorConstants.DependentRequired)]
// ReSharper disable once UnusedType.Global
public class DependentRequiredSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<GroupedIfThenElseSchemaRuleValidator>
{
    public override GroupedIfThenElseSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath.FullPath, out var dependentRequiredSchema) == false)
            return null;

        var propertyNames = dependentRequiredSchema.GetPropertyNames();
        if (propertyNames.Length == 0)
            return null;

        List<IfThenElseSchemaRuleValidator> dependentRequires = null;
        foreach (var propertyName in propertyNames)
        {
            if (SchemaValidationHelper.TryGetArray(dependentRequiredSchema, propertyName, schemaPath.FullPath, out var requiredSchema) == false)
                throw new InvalidOperationException(
                    $"Should not happen. {propertyName} exists and wrong type should throw {nameof(InvalidSchemaValidationDefinitionException)}");
            
            if(requiredSchema.Length == 0)
                continue;
                    
            var propertySchemaPath = schemaPath + propertyName; 
            var ifRequiredValidator = new RequiredSchemaRuleValidator(propertyName);
            var ifValidator = new SelfObjectElementSchemaRuleValidator(null, [ifRequiredValidator], propertySchemaPath);
            
            var thenRequiredValidator = new RequiredSchemaRuleValidator(requiredSchema);
            var thenValidator = new SelfObjectElementSchemaRuleValidator(null, [thenRequiredValidator], propertySchemaPath);

            (dependentRequires ??= new List<IfThenElseSchemaRuleValidator>()).Add(new IfThenElseSchemaRuleValidator(ifValidator, thenValidator));
        }

        if (dependentRequires == null)
            return null;
        
        return new GroupedIfThenElseSchemaRuleValidator(dependentRequires.ToArray());
    }
}

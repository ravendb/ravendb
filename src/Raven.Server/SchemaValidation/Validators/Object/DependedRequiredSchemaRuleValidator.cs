using System.Collections.Generic;
using Raven.Server.SchemaValidation.ErrorMessage;
using Raven.Server.SchemaValidation.Validators.Untyped;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

[SchemaRule(SchemaValidatorConstants.DependentRequired)]
// ReSharper disable once UnusedType.Global
public class DependentRequiredSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<GroupedIfThenElseSchemaRuleValidator>
{
    public override GroupedIfThenElseSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        schemaPath += Rule;
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath, out var dependentRequiredSchema) == false)
            return null;

        if (dependentRequiredSchema.Count == 0)
            return null;
        List<IfThenElseSchemaRuleValidator> dependentRequires = null;
        var prop = default(BlittableJsonReaderObject.PropertyDetails);
        for (int i = 0; i < dependentRequiredSchema.Count; i++)
        {
            dependentRequiredSchema.GetPropertyByIndex(i, ref prop);
            var requiredSchema = SchemaValidationHelper.CheckTypeAndThrow<BlittableJsonReaderArray>(prop.Value, schemaPath);
            var requiredProperties = SchemaValidationHelper.CheckBlittableArrayElementTypesAndThrow<LazyStringValue>(requiredSchema, schemaPath);
            if(requiredProperties == null)
                continue;
                    
            var propertySchemaPath = schemaPath + prop.Name; 
            var ifRequiredValidator = new RequiredSchemaRuleValidator(prop.Name);
            var ifValidator = new ElementSchemaRuleValidator(null, [ifRequiredValidator], propertySchemaPath);
            
            var thenRequiredValidator = new RequiredSchemaRuleValidator(requiredProperties);
            var thenValidator = new ElementSchemaRuleValidator(null, [thenRequiredValidator], propertySchemaPath);

            (dependentRequires ??= []).Add(new IfThenElseSchemaRuleValidator(ifValidator, thenValidator));
        }

        if (dependentRequires == null)
            return null;
        
        return new GroupedIfThenElseSchemaRuleValidator(dependentRequires.ToArray());
    }
}

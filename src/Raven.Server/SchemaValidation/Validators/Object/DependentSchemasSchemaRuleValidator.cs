using System.Collections.Generic;
using Raven.Server.SchemaValidation.ErrorMessage;
using Raven.Server.SchemaValidation.Validators.Untyped;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

[SchemaRule(SchemaValidatorConstants.DependentSchemas)]
// ReSharper disable once UnusedType.Global
public class DependentSchemasSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<GroupedIfThenElseSchemaRuleValidator>
{
    public override GroupedIfThenElseSchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath, RefSchemas refSchemas)
    {
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath.FullPath, out var dependentRequiredSchema) == false)
            return null;

        schemaPath += Rule;
        
        if (dependentRequiredSchema.Count == 0)
            return null;

        List<IfThenElseSchemaRuleValidator> dependentRequires = null;
        var prop = default(BlittableJsonReaderObject.PropertyDetails);
        for (int i = 0; i < dependentRequiredSchema.Count; i++)
        {
            dependentRequiredSchema.GetPropertyByIndex(i, ref prop);
            var dependentSchemas = SchemaValidationHelper.CheckTypeAndThrow<BlittableJsonReaderObject>(prop.Name, prop.Value, schemaPath.FullPath);
            
            var propertySchemaPath = schemaPath + prop.Name;
            
            if(dependentSchemas.Count == 0)
                continue;
                    
            var ifRequiredValidator = new RequiredSchemaRuleValidator(prop.Name);
            var ifValidator = new ElementSchemaRuleValidator(null, [ifRequiredValidator], propertySchemaPath);
            
            var thenValidator = ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(dependentSchemas, propertySchemaPath, refSchemas);

            (dependentRequires ??= []).Add(new IfThenElseSchemaRuleValidator(ifValidator, thenValidator));
        }

        if (dependentRequires == null)
            return null;
        
        return new GroupedIfThenElseSchemaRuleValidator(dependentRequires.ToArray());
    }}

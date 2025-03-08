using System;
using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Server.SchemaValidation.Validators.Object;

[SchemaRule(SchemaValidatorConstants.dependentSchemas)]
// ReSharper disable once UnusedType.Global
public class DependentSchemasSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<IfThenElseSchemaRuleValidator>
{
    public override ISchemaRuleValidator Create(BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
    {
        if (SchemaValidationHelper.TryGetObject(schemaDefinition, Rule, schemaPath.FullPath, out var dependentRequiredSchema) == false)
            return null;

        schemaPath += Rule;
        
        var propertyNames = dependentRequiredSchema.GetPropertyNames();
        if (propertyNames.Length == 0)
            return null;

        List<IfThenElseSchemaRuleValidator> dependentRequires = null;
        foreach (var propertyName in propertyNames)
        {
            if (SchemaValidationHelper.TryGetObject(dependentRequiredSchema, propertyName, schemaPath.FullPath, out var dependentSchemas) == false)
                throw new InvalidOperationException(
                    $"Should not happen. {propertyName} exists and wrong type should throw {nameof(InvalidSchemaValidationDefinitionException)}");
            
            var propertySchemaPath = schemaPath + propertyName;
            
            if(dependentSchemas.GetPropertyNames().Length == 0)
                continue;
                    
            var ifRequiredValidator = new RequiredSchemaRuleValidator(propertyName);
            var ifValidator = new SelfElementSchemaRuleValidator(null, [ifRequiredValidator], propertySchemaPath);
            
            var thenValidator = ElementSchemaRuleValidatorFactory.CreateSelfElementSchemaRuleValidator(dependentSchemas, propertySchemaPath);

            (dependentRequires ??= []).Add(new IfThenElseSchemaRuleValidator(ifValidator, thenValidator));
        }

        if (dependentRequires == null)
            return null;
        
        return new GroupedIfThenElseSchemaRuleValidator(dependentRequires.ToArray());
    }}

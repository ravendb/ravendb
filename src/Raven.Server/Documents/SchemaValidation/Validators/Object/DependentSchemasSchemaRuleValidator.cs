using System.Collections.Generic;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;
using Raven.Server.Documents.SchemaValidation.Validators.Untyped;
using Sparrow.Json;

namespace Raven.Server.Documents.SchemaValidation.Validators.Object;

[SchemaRule(SchemaValidatorConstants.DependentSchemas)]
// ReSharper disable once UnusedType.Global
public class DependentSchemasSchemaRuleValidatorFactory : SchemaRuleValidatorFactory<GroupedIfThenElseSchemaRuleValidator>
{
    public override GroupedIfThenElseSchemaRuleValidator Create(SchemaBuilderContext context, BlittableJsonReaderObject schemaDefinition, SchemaPath schemaPath)
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
            var dependentSchemas = SchemaValidationHelper.CheckTypeAndThrow<BlittableJsonReaderObject>(prop.Value, schemaPath);

            var propertySchemaPath = schemaPath + prop.Name;

            if (dependentSchemas.Count == 0)
                continue;

            var ifRequiredValidator = new RequiredSchemaRuleValidator(prop.Name);
            var ifValidator = new ElementSchemaRuleValidator([ifRequiredValidator], propertySchemaPath);

            var thenValidator = ElementSchemaRuleValidatorFactory.CreateElementSchemaRuleValidator(context, dependentSchemas, propertySchemaPath);

            (dependentRequires ??= []).Add(new IfThenElseSchemaRuleValidator(ifValidator, thenValidator));
        }

        if (dependentRequires == null)
            return null;

        return new GroupedIfThenElseSchemaRuleValidator(dependentRequires.ToArray());
    }
}

import { DocumentSchemaValidatorConfig } from "components/pages/database/settings/documentSchema/store/documentSchemaSlice";
import { DocumentSchemaFormData } from "components/pages/database/settings/documentSchema/DocumentSchema";
import SchemaValidationConfiguration = Raven.Client.Documents.Operations.SchemaValidation.SchemaValidationConfiguration;

const mapToSchemaValidationConfigurationDto = (
    formData: DocumentSchemaValidatorConfig[]
): SchemaValidationConfiguration => {
    return {
        Disabled: false,
        ValidatorsPerCollection: Object.fromEntries(
            formData.map((x) => [
                x.Name,
                {
                    Disabled: x.Disabled,
                    Schema: x.Schema,
                    LastModifiedTime: x.LastModifiedTime ?? new Date().toISOString(),
                },
            ])
        ),
    };
};

const maptoDocumentSchemaValidatorConfigDto = (formData: DocumentSchemaFormData): DocumentSchemaValidatorConfig => {
    return {
        Name: formData.collection,
        Disabled: false,
        Schema: formData.schema,
        LastModifiedTime: new Date().toISOString(),
    };
};

export const documentSchemaUtils = { mapToSchemaValidationConfigurationDto, maptoDocumentSchemaValidatorConfigDto };

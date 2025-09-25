import { DocumentSchemaValidatorConfig } from "components/pages/database/settings/documentSchema/store/documentSchemaSlice";
import { DocumentSchemaFormData } from "components/pages/database/settings/documentSchema/DocumentSchema";
import SchemaValidationConfiguration = Raven.Client.Documents.Operations.SchemaValidation.SchemaValidationConfiguration;
import genUtils from "common/generalUtils";

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

const mapToDocumentSchemaValidatorConfigDto = (formData: DocumentSchemaFormData): DocumentSchemaValidatorConfig => {
    return {
        Name: formData.collection,
        Disabled: false,
        Schema: genUtils.stringify(JSON.parse(formData.schema)),
        LastModifiedTime: new Date().toISOString(),
    };
};

export const documentSchemaUtils = { mapToSchemaValidationConfigurationDto, mapToDocumentSchemaValidatorConfigDto };

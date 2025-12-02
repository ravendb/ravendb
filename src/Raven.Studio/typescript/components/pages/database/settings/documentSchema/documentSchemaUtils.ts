import { DocumentSchemaValidatorConfig } from "components/pages/database/settings/documentSchema/store/documentSchemaSlice";
import { DocumentSchemaFormData } from "components/pages/database/settings/documentSchema/DocumentSchema";
import SchemaValidationConfiguration = Raven.Client.Documents.Operations.SchemaValidation.SchemaValidationConfiguration;
import genUtils from "common/generalUtils";
import { ValidateSchemaFormData } from "components/pages/database/settings/documentSchema/partials/ValidationSchemaViewSheetPanel";

const mapToSchemaValidationConfigurationDto = (
    formData: DocumentSchemaValidatorConfig[],
    isGlobalDisabled?: boolean
): SchemaValidationConfiguration => {
    return {
        Disabled: isGlobalDisabled,
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

const mapToValidateSchemaRequestDto = (
    validator: Pick<DocumentSchemaValidatorConfig, "Name" | "Schema">,
    formData: ValidateSchemaFormData
) => {
    if (!validator) {
        return null;
    }

    return {
        Collection: validator.Name,
        SchemaDefinition: validator.Schema,
        MaxDocumentsToValidate: formData.isTestSettingsEnabled ? formData.maxDocumentsToValidate : null,
        MaxErrorMessages: formData.isTestSettingsEnabled ? formData.maxErrorMessages : null,
    };
};

export const documentSchemaUtils = { mapToSchemaValidationConfigurationDto, mapToDocumentSchemaValidatorConfigDto, mapToValidateSchemaRequestDto };

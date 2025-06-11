import { TermsForField } from "components/pages/database/indexes/list/terms/useIndexTerms";

const createTermsForField = (
    fieldName: string,
    type: IndexEntriesFieldType,
    termType: IndexEntriesValueType
): TermsForField => {
    return {
        fromValue: null,
        name: fieldName,
        hasMoreTerms: true,
        terms: [],
        type,
        termType,
        loadError: "",
    };
};

const getTermsLoadedAmount = (indexTerms: TermsForField[]) => {
    return indexTerms.reduce((acc, curr) => acc + curr.terms.length, 0);
};

const getTermsFields = (perNodeFields: getIndexEntriesFieldsCommandResult[]) => {
    const allFields = perNodeFields;

    const dynamicFields = new Map<string, getIndexEntriesFieldsCommandResult>();
    const staticFields = new Map<string, getIndexEntriesFieldsCommandResult>();

    allFields.forEach((field) => {
        switch (field.FieldType) {
            case "Dynamic":
                dynamicFields.set(field.Name, field);
                break;
            case "Static":
                staticFields.set(field.Name, field);
                break;
            default:
                break;
        }
    });

    const processedStaticFields = Array.from(staticFields.values()).map((field) =>
        createTermsForField(field.Name, field.FieldType, field.ValueType)
    );

    const processedDynamicFields = Array.from(dynamicFields.values()).map((field) =>
        createTermsForField(field.Name, field.FieldType, field.ValueType)
    );

    return processedStaticFields.concat(processedDynamicFields);
};

export { createTermsForField, getTermsLoadedAmount, getTermsFields };

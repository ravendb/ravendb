import { FieldType, TermsForField } from "components/pages/database/indexes/list/terms/useIndexTerms";

const createTermsForField = (fieldName: string, type: FieldType): TermsForField => {
    return {
        fromValue: null,
        name: fieldName,
        hasMoreTerms: true,
        terms: [],
        type,
        loadError: "",
    };
};

const getTermsLoadedAmount = (indexTerms: TermsForField[]) => {
    return indexTerms.reduce((acc, curr) => acc + curr.terms.length, 0);
};

const getTermsFields = (perNodeFields: getIndexEntriesFieldsCommandResult[]) => {
    const dynamicFields = new Set<string>();
    const staticFields = new Set<string>();

    perNodeFields.forEach((fields) => {
        fields.Dynamic.forEach((d) => dynamicFields.add(d));
        fields.Static.forEach((d) => staticFields.add(d));
    });

    const joinedResult: getIndexEntriesFieldsCommandResult = {
        Dynamic: Array.from(dynamicFields),
        Static: Array.from(staticFields),
    };

    const processedStaticFields = joinedResult.Static.map((fieldName) => createTermsForField(fieldName, "static"));
    const processedDynamicFields = joinedResult.Dynamic.map((fieldName) => createTermsForField(fieldName, "dynamic"));

    return processedStaticFields.concat(processedDynamicFields);
};

export { createTermsForField, getTermsLoadedAmount, getTermsFields };

import * as yup from "yup";

interface NameSchemaContext {
    isForNewConnection: boolean;
    usedNames: string[];
}

export const connectionStringsUtils = {
    nameSchema: yup
        .string()
        .nullable()
        .required()
        .test("is-name-unique", "Name must be unique", (value, ctx) => {
            const { isForNewConnection, usedNames } = ctx.options.context as NameSchemaContext;

            if (isForNewConnection) {
                return !usedNames.includes(value);
            }

            return true;
        }),
};

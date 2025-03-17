import * as yup from "yup";

export interface ConnectionStringsNameContext {
    isForNewConnection: boolean;
    usedNames: string[];
}

export const connectionStringsUtils = {
    nameSchema: yup
        .string()
        .nullable()
        .required()
        .test("is-name-unique", "Name must be unique", (value, ctx) => {
            const { isForNewConnection, usedNames } = ctx.options.context as ConnectionStringsNameContext;

            if (isForNewConnection) {
                return !usedNames.includes(value);
            }

            return true;
        }),
};

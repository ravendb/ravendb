import * as yup from "yup";

export interface ConnectionStringsNameContext {
    isForNewConnection: boolean;
    usedNames: string[];
}

export const serverWideConnectionStringPrefix = "Server Wide Connection String";

export function getServerWideShortName(fullName: string): string {
    return fullName.slice(serverWideConnectionStringPrefix.length + 2);
}

export const connectionStringsUtils = {
    nameSchema: yup
        .string()
        .nullable()
        .required()
        .test(
            "no-server-wide-prefix",
            `Name cannot start with the prefix "${serverWideConnectionStringPrefix}"`,
            (value) => {
                if (!value) {
                    return true;
                }

                return !value.trim().toLowerCase().startsWith(serverWideConnectionStringPrefix.toLowerCase());
            }
        )
        .test("is-name-unique", "Name must be unique", (value, ctx) => {
            const { isForNewConnection, usedNames } = ctx.options.context as ConnectionStringsNameContext;

            if (isForNewConnection) {
                return !usedNames.includes(value);
            }

            return true;
        }),
};

import * as yup from "yup";

export const dataDirectoryStepSchema = yup.object({
    isDefault: yup.boolean(),
    directory: yup.string().when("isDefault", {
        is: false,
        then: (schema) =>
            schema
                .required()
                .trim()
                .strict()
                .max(248)
                .test(
                    "forbidden-characters",
                    `Path can't contain any of the following characters: * ? " < > |`,
                    (value) => {
                        return /^[^*?"<>|]*$/.test(value);
                    }
                )
                .test("forbidden-characters", "The name is forbidden for use!", (value) => {
                    return !/^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i.test(value);
                })
                .test(
                    "forbidden-prefix",
                    "The path is illegal! Paths in RavenDB can't start with 'appdrive:', '~' or '$home'",
                    (value) => {
                        if (!value) {
                            return true;
                        }

                        const valueLower = value.toLowerCase();
                        return (
                            !valueLower.startsWith("~") &&
                            !valueLower.startsWith("$home") &&
                            !valueLower.startsWith("appdrive:")
                        );
                    }
                ),
    }),
});

export const encryptionStepSchema = yup.object({
    key: yup.string().when("$isEncrypted", {
        is: true,
        then: (schema) => schema.base64().required(),
    }),
    isKeySaved: yup.boolean().when("$isEncrypted", {
        is: true,
        then: (schema) => schema.oneOf([true], "Encryption key must be saved"),
    }),
});

export type Encryption = yup.InferType<typeof encryptionStepSchema>;

export const databaseNameSchema = yup
    .string()
    .trim()
    .strict()
    .required()
    .test("db-exists", "Database already exists", (value, ctx) => {
        return !ctx.options.context.usedDatabaseNames.some(
            (name: string) => name.toLowerCase() === value.toLowerCase()
        );
    });

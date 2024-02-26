import * as yup from "yup";

export const dataDirectoryStepSchema = yup.object({
    isDefault: yup.boolean(),
    directory: yup.string().when("isDefault", {
        is: false,
        then: (schema) => schema.required(),
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

import * as yup from "yup";

export const pathsConfigurationsSchema = yup.object({
    isDefault: yup.boolean(),
    path: yup.string().when("isDefault", {
        is: false,
        then: (schema) => schema.required(),
    }),
});

export const encryptionSchema = yup.object({
    // TODO base64
    key: yup.string().when("$isEncrypted", {
        is: true,
        then: (schema) => schema.required(),
    }),
    isKeySaved: yup.boolean().when("$isEncrypted", {
        is: true,
        then: (schema) => schema.oneOf([true], "Encryption key must be saved"),
    }),
});

export type Encryption = yup.InferType<typeof encryptionSchema>;

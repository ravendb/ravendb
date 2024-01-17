import * as yup from "yup";

export const pathsConfigurationsSchema = yup.object({
    isPathDefault: yup.boolean(),
    path: yup.string().when("isPathDefault", {
        is: false,
        then: (schema) => schema.required(),
    }),
});

export type PathsConfigurations = yup.InferType<typeof pathsConfigurationsSchema>;

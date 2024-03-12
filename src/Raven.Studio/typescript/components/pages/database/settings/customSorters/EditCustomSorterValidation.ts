import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

const schema = yup.object({
    name: yup.string().required(),
    code: yup
        .string()
        .required()
        .test("class-name", "Class name must be the same as your sorter name above", (code, ctx) => {
            const regex = new RegExp("class " + ctx.parent.name + "[{\\W]");
            return regex.test(code);
        }),
});

export const customSorterYupResolver = yupResolver(schema);
export type CustomSorterFormData = yup.InferType<typeof schema>;

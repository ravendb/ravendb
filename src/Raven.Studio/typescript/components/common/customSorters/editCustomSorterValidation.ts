import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";

function buildSchema(takenNames: string[]) {
    return yup.object({
        name: yup.string().required().notOneOf(takenNames, "A sorter with this name already exists"),
        code: yup
            .string()
            .required()
            .test("class-name", "Class name must be the same as your sorter name above", (code, ctx) => {
                const regex = new RegExp("class " + ctx.parent.name + "[{\\W]");
                return regex.test(code);
            }),
    });
}

export function createCustomSorterYupResolver(takenNames: string[] = []) {
    return yupResolver(buildSchema(takenNames));
}

export type CustomSorterFormData = yup.InferType<ReturnType<typeof buildSchema>>;

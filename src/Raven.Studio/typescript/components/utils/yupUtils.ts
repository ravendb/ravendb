import * as yup from "yup";

/* eslint-disable @typescript-eslint/no-explicit-any */
export type ConditionalSchema<T> = T extends string
    ? yup.StringSchema
    : T extends number
    ? yup.NumberSchema
    : T extends boolean
    ? yup.BooleanSchema
    : T extends Date
    ? yup.DateSchema
    : T extends Array<T[any]>
    ? yup.ArraySchema<Array<Partial<T[any]>>, yup.AnyObject, "", "">
    : T extends Record<any, any>
    ? yup.AnyObjectSchema
    : yup.AnySchema;
/* eslint-enable @typescript-eslint/no-explicit-any */

export type YupShape<Fields> = {
    [Key in keyof Fields]: ConditionalSchema<Fields[Key]>;
};

export type YupObjectSchema<T> = yup.ObjectSchema<T, yup.AnyObject, yup.DefaultFromShape<YupShape<T>>, "">;

export function yupObjectSchema<T>(x: YupShape<Required<T>>): YupObjectSchema<T> {
    return yup.object(x) as undefined;
}

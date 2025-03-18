import * as yup from "yup";

const filterSchema = yup.object({
    minLevel: yup.string<Sparrow.Logging.LogLevel>().nullable().required(),
    maxLevel: yup.string<Sparrow.Logging.LogLevel>().nullable().required(),
    condition: yup.string().nullable().required(),
    action: yup.string<Sparrow.Logging.LogFilterAction>().nullable().required(),
});

const initialFilter: yup.InferType<typeof filterSchema> = {
    minLevel: "Trace",
    maxLevel: "Fatal",
    condition: null,
    action: null,
};

export const adminLogsUtils = {
    filtersSchema: yup.array().of(filterSchema),
    initialFilter,
};

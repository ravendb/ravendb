import { SelectOption } from "components/common/select/Select";

export const openAiModelOptions: SelectOption[] = [
    "text-embedding-3-small",
    "text-embedding-3-large",
    "text-embedding-ada-002",
].map((x) => ({
    label: x,
    value: x,
}));

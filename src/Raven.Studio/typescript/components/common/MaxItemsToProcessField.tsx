import { Control, FieldPath, FieldValues, PathValue } from "react-hook-form";
import { DEFAULT_ITEMS_TO_PROCESS } from "common/constants/maxItemsToProcess";
import { i18n } from "common/i18n/i18n";
import { FormInputWithDefault } from "components/common/FieldWithDefault";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface MaxItemsToProcessFieldProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: Control<TFieldValues>;
    name: TName;
    disabled: boolean;
}

export default function MaxItemsToProcessField<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
>({ control, name, disabled }: MaxItemsToProcessFieldProps<TFieldValues, TName>) {
    const t = useStudioTranslation("maxItemsToProcessField");
    const optimalValueLabel = DEFAULT_ITEMS_TO_PROCESS.toLocaleString(i18n.language);

    return (
        <FormInputWithDefault
            control={control}
            name={name}
            label={t("label")}
            disabled={disabled}
            optimalValue={DEFAULT_ITEMS_TO_PROCESS as PathValue<TFieldValues, TName>}
            optimalValueLabel={optimalValueLabel}
            optimalValueHint={t("optimalHint", { items: optimalValueLabel })}
            defaultValueLabel={t("unlimited")}
            type="number"
            addon={t("itemsAddon")}
        />
    );
}

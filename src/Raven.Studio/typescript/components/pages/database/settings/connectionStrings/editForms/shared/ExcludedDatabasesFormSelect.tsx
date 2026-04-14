import { FormLabel, FormSelect } from "components/common/Form";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { Control, FieldValues, Path } from "react-hook-form";
import { useMemo } from "react";

interface ExcludedDatabasesFormSelectProps<TFieldValues extends FieldValues> {
    control: Control<TFieldValues>;
    name: Path<TFieldValues>;
}

export default function ExcludedDatabasesFormSelect<TFieldValues extends FieldValues>({
    control,
    name,
}: ExcludedDatabasesFormSelectProps<TFieldValues>) {
    const allDatabases = useAppSelector(databaseSelectors.allDatabases);
    const databaseOptions = useMemo(
        () => allDatabases.filter((x) => !x.isDisabled).map((x) => ({ value: x.name, label: x.name })),
        [allDatabases]
    );

    return (
        <div className="mb-2">
            <FormLabel>Excluded Databases</FormLabel>
            <FormSelect
                control={control}
                name={name}
                isMulti
                options={databaseOptions}
                placeholder="Select databases to exclude (optional)"
                isClearable
            />
        </div>
    );
}

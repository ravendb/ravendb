import { FormLabel, FormSelect } from "components/common/Form";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { Control, FieldValues, Path } from "react-hook-form";
import { useMemo } from "react";
import { ConnectionStringUsage } from "../../connectionStringsTypes";

interface ExcludedDatabasesFormSelectProps<TFieldValues extends FieldValues> {
    control: Control<TFieldValues>;
    name: Path<TFieldValues>;
    usedBy?: ConnectionStringUsage[];
}

export default function ExcludedDatabasesFormSelect<TFieldValues extends FieldValues>({
    control,
    name,
    usedBy,
}: ExcludedDatabasesFormSelectProps<TFieldValues>) {
    const allDatabases = useAppSelector(databaseSelectors.allDatabases);
    const usedByDatabaseNames = useMemo(() => new Set(usedBy?.map((x) => x.databaseName).filter(Boolean)), [usedBy]);
    const databaseOptions = useMemo(
        () =>
            allDatabases.map((x) => ({
                value: x.name,
                label: x.name,
                isDisabled: usedByDatabaseNames.has(x.name),
            })),
        [allDatabases, usedByDatabaseNames]
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

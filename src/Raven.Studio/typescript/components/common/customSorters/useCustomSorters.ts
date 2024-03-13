import { CustomSorterFormData } from "components/common/customSorters/editCustomSorterValidation";
import { useState } from "react";

export function useCustomSorters() {
    const [sorters, setSorters] = useState<CustomSorterFormData[]>([]);

    const addNewSorter = () => {
        setSorters((prev) => [{ name: "", code: "" } satisfies CustomSorterFormData, ...prev]);
    };

    const removeSorter = (idx: number) => {
        setSorters((prev) => prev.filter((_, i) => i !== idx));
    };

    return {
        sorters,
        setSorters,
        addNewSorter,
        removeSorter,
        mapFromDto,
    };
}

function mapFromDto(dto: Raven.Client.Documents.Queries.Sorting.SorterDefinition[]): CustomSorterFormData[] {
    return dto.map((x) => ({ code: x.Code, name: x.Name }) satisfies CustomSorterFormData);
}

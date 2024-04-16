import { CustomSorterFormData } from "components/common/customSorters/editCustomSorterValidation";
import { useState } from "react";

export interface CustomSorter extends CustomSorterFormData {
    id: string;
}

export function useCustomSorters() {
    const [sorters, setSorters] = useState<CustomSorter[]>([]);

    const addNewSorter = () => {
        setSorters((prev) => [{ id: createId(), name: "", code: "" } satisfies CustomSorter, ...prev]);
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

function mapFromDto(dto: Raven.Client.Documents.Queries.Sorting.SorterDefinition[]): CustomSorter[] {
    return dto.map((x) => ({ id: createId(), code: x.Code, name: x.Name }) satisfies CustomSorter);
}

function createId() {
    return _.uniqueId("custom-sorter");
}

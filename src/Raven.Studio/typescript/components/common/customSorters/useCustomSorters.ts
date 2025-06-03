import { CustomSorterFormData } from "components/common/customSorters/editCustomSorterValidation";
import { useState } from "react";

export interface CustomSorter extends CustomSorterFormData {
    id: string;
    isSaved: boolean;
}

export function useCustomSorters() {
    const [sorters, setSorters] = useState<CustomSorter[]>([]);

    const addNewSorter = () => {
        setSorters((prev) => [{ id: createId(), name: "", code: "", isSaved: false } satisfies CustomSorter, ...prev]);
    };

    const removeSorter = (idx: number) => {
        setSorters((prev) => prev.filter((_, i) => i !== idx));
    };

    const markAsSaved = (idx: number) => {
        setSorters((prev) => prev.map((x, i) => (i === idx ? { ...x, isSaved: true } : x)));
    };

    return {
        sorters,
        setSorters,
        addNewSorter,
        removeSorter,
        mapFromDto,
        markAsSaved,
    };
}

function mapFromDto(dto: Raven.Client.Documents.Queries.Sorting.SorterDefinition[]): CustomSorter[] {
    return dto.map((x) => ({ id: createId(), code: x.Code, name: x.Name, isSaved: true }) satisfies CustomSorter);
}

function createId() {
    return _.uniqueId("custom-sorter");
}

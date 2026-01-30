import { MultiCheckboxToggle } from "components/common/toggles/MultiCheckboxToggle";
import { InputItem } from "components/models/common";
import Select, { SelectOption } from "components/common/select/Select";

export type DocumentSchemaStatus = "enabled" | "disabled";

interface DocumentSchemaFilterProps {
    selectedCollections: SelectOption[];
    setSelectedCollections: (collections: SelectOption[]) => void;
    collectionOptions: SelectOption[];
    selectedStatuses: DocumentSchemaStatus[];
    setSelectedStatuses: (statuses: DocumentSchemaStatus[]) => void;
    schemasCount: number;
    isLoading?: boolean;
}

const filterByStatusOptions: InputItem<DocumentSchemaStatus>[] = [
    { value: "enabled", label: "Enabled" },
    { value: "disabled", label: "Disabled" },
];

export default function DocumentSchemaFilter({
    selectedCollections,
    setSelectedCollections,
    collectionOptions,
    selectedStatuses,
    setSelectedStatuses,
    schemasCount,
    isLoading,
}: DocumentSchemaFilterProps) {
    return (
        <div className="hstack flex-wrap align-items-end gap-3 my-3 justify-content-end">
            <div className="flex-grow">
                <div className="small-label ms-1 mb-1">Filter by collection</div>
                <Select
                    isLoading={isLoading}
                    options={collectionOptions}
                    isMulti
                    value={selectedCollections}
                    onChange={setSelectedCollections}
                    placeholder="All collections"
                    isClearable
                    isRoundedPill
                />
            </div>

            <MultiCheckboxToggle
                inputItems={filterByStatusOptions}
                label="Filter by state"
                selectedItems={selectedStatuses}
                setSelectedItems={setSelectedStatuses}
                selectAll
                selectAllLabel="All"
                selectAllCount={schemasCount}
            />
        </div>
    );
}

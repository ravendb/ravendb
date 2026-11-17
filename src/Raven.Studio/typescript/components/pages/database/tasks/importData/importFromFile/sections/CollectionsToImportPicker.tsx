import React, { useState } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Switch } from "components/common/Checkbox";
import { ImportFromFileFormData } from "../importFromFileValidation";
import CollectionNamesEditor from "./CollectionNamesEditor";

export default function CollectionsToImportPicker() {
    const { control, setValue, getValues } = useFormContext<ImportFromFileFormData>();

    const includedCollections = useWatch({ control, name: "collections.includedCollections" }) ?? [];

    const [manualCollections, setManualCollections] = useState<string[]>(
        () => getValues("collections.includedCollections") ?? []
    );

    const areAllSelected =
        manualCollections.length > 0 && manualCollections.every((name) => includedCollections.includes(name));

    const setIncludedCollections = (names: string[]) =>
        setValue("collections.includedCollections", names, { shouldDirty: true });

    const addCollections = (added: string[]) => {
        setManualCollections((prev) => [...prev, ...added]);
        setIncludedCollections([...(getValues("collections.includedCollections") ?? []), ...added]);
    };

    const removeCollection = (name: string) => {
        setManualCollections((prev) => prev.filter((x) => x !== name));
        setIncludedCollections(includedCollections.filter((x) => x !== name));
    };

    const toggleCollection = (name: string, include: boolean) => {
        if (!include) {
            setIncludedCollections(includedCollections.filter((x) => x !== name));
            return;
        }

        setIncludedCollections([...includedCollections, name]);
    };

    const toggleAll = () => {
        setIncludedCollections(areAllSelected ? [] : [...manualCollections]);
    };

    return (
        <div className="mt-4">
            <CollectionNamesEditor
                names={manualCollections}
                onAdd={addCollections}
                onRemove={removeCollection}
                inputLabel="Collections to import"
                listTitle="Collection name"
                emptyText="No collections added. Type a collection name from the imported file above and click Add."
                headerAction={
                    <div className="d-flex align-items-center gap-2">
                        <span>Select all</span>
                        <Switch
                            id="select-all-collections"
                            className="m-0"
                            color="primary"
                            disabled={manualCollections.length === 0}
                            selected={areAllSelected}
                            toggleSelection={toggleAll}
                        />
                    </div>
                }
                renderName={(name) => (
                    <Switch
                        id={`import-collection-${encodeURIComponent(name)}`}
                        color="primary"
                        selected={includedCollections.includes(name)}
                        toggleSelection={(e) => toggleCollection(name, e.target.checked)}
                        title={name}
                    >
                        {name}
                    </Switch>
                )}
            />
        </div>
    );
}

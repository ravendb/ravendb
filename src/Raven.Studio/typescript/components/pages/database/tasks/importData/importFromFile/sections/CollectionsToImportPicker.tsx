import React, { useEffect, useState } from "react";
import { useForm, useFormContext, useFormState, useWatch } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import Button from "react-bootstrap/Button";
import InputGroup from "react-bootstrap/InputGroup";
import { Icon } from "components/common/Icon";
import { Switch } from "components/common/Checkbox";
import { FormInput } from "components/common/Form";
import InnerForm from "components/common/InnerForm";
import { ImportFromFileFormData } from "../importFromFileValidation";

/**
 * Collections to import, typed in by hand: the server cannot list a dump file's collections before
 * the upload, so there is nothing to pick from. Two separate inputs on purpose - one adds a name,
 * the other only narrows the rows already added.
 */
export default function CollectionsToImportPicker() {
    const { control: importControl, setValue, getValues } = useFormContext<ImportFromFileFormData>();

    const includedCollections = useWatch({ control: importControl, name: "collections.includedCollections" }) ?? [];

    // Rows live in local state so deselecting only turns the toggle off - the trash icon removes
    // the row entirely. The picker unmounts whenever "Import all collections" is active, so the
    // rows are re-seeded from the form's included list on mount.
    const [manualCollections, setManualCollections] = useState<string[]>(
        () => getValues("collections.includedCollections") ?? []
    );
    const [addedCount, setAddedCount] = useState(0);

    // A form of its own, separate from the page's import form: its only field is the name being
    // typed, which is never submitted to the server - it just appends a row.
    const addCollectionForm = useForm({
        resolver: yupResolver(getAddCollectionSchema(manualCollections)),
        defaultValues: { collectionName: "" },
    });
    const { control: addControl, handleSubmit, setValue: setAddValue, clearErrors, setFocus } = addCollectionForm;

    const filterForm = useForm<FilterCollectionsFormData>({ defaultValues: { filter: "" } });
    const collectionFilter = useWatch({ control: filterForm.control, name: "filter" }) ?? "";

    const filteredCollections = manualCollections.filter((name) =>
        name.toLowerCase().includes(collectionFilter.toLowerCase())
    );

    const areAllFilteredSelected =
        filteredCollections.length > 0 && filteredCollections.every((name) => includedCollections.includes(name));

    const setIncludedCollections = (names: string[]) =>
        setValue("collections.includedCollections", names, { shouldDirty: true });

    const addCollection = ({ collectionName }: AddCollectionFormData) => {
        const trimmed = collectionName.trim();

        setManualCollections((prev) => [...prev, trimmed]);
        setIncludedCollections([...includedCollections, trimmed]);
        setAddedCount((prev) => prev + 1);
        // setValue rather than reset: it clears the field without unregistering it, so the input
        // keeps the ref that setFocus needs.
        setAddValue("collectionName", "");
        clearErrors("collectionName");
    };

    // FormInput disables itself while the add form is submitting, and a disabled input silently
    // ignores focus() - so refocusing is deferred until isSubmitting drops back to false. Counting
    // additions rather than watching the list length keeps the trash icon from pulling focus back
    // into the input.
    const { isSubmitting: isAddSubmitting } = useFormState({ control: addControl });

    useEffect(() => {
        if (isAddSubmitting) {
            return;
        }

        setFocus("collectionName");
    }, [addedCount, isAddSubmitting, setFocus]);

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

    const toggleAllFiltered = () => {
        if (areAllFilteredSelected) {
            setIncludedCollections(includedCollections.filter((name) => !filteredCollections.includes(name)));
            return;
        }

        const missing = filteredCollections.filter((name) => !includedCollections.includes(name));
        setIncludedCollections([...includedCollections, ...missing]);
    };

    const getEmptyListMessage = () => {
        if (manualCollections.length === 0) {
            return "No collections added. Type a collection name from the imported file above and click Add.";
        }

        return "No added collection matches the filter.";
    };

    return (
        <div className="mt-4">
            <InnerForm onSubmit={handleSubmit(addCollection)} className="mb-3">
                <InputGroup>
                    <FormInput
                        type="text"
                        control={addControl}
                        name="collectionName"
                        placeholder="Type a collection name from the imported file"
                    />
                    <Button variant="secondary" className="text-nowrap" onClick={handleSubmit(addCollection)}>
                        <Icon icon="plus" /> Add
                    </Button>
                </InputGroup>
            </InnerForm>
            {manualCollections.length > 0 && (
                <div className="mb-2">
                    <FormInput
                        type="text"
                        control={filterForm.control}
                        name="filter"
                        size="sm"
                        placeholder="Filter added collections"
                    />
                </div>
            )}
            <div className="import-list-header mb-2">
                <span className="flex-grow-1 fw-semibold">Collection name</span>
                <div className="d-flex align-items-center gap-2">
                    <span>Select all</span>
                    <Switch
                        id="select-all-collections"
                        className="m-0"
                        disabled={filteredCollections.length === 0}
                        selected={areAllFilteredSelected}
                        toggleSelection={toggleAllFiltered}
                    />
                </div>
            </div>
            <div className="import-collections-list">
                {filteredCollections.length === 0 && (
                    <div className="import-list-item text-muted">{getEmptyListMessage()}</div>
                )}
                {filteredCollections.map((name) => (
                    <div key={name} className="import-list-item">
                        <Switch
                            id={`import-collection-${encodeURIComponent(name)}`}
                            selected={includedCollections.includes(name)}
                            toggleSelection={(e) => toggleCollection(name, e.target.checked)}
                        >
                            {name}
                        </Switch>
                        <Button
                            variant="link"
                            size="sm"
                            className="p-0 text-danger"
                            title="Remove collection"
                            onClick={() => removeCollection(name)}
                        >
                            <Icon icon="trash" margin="m-0" />
                        </Button>
                    </div>
                ))}
            </div>
        </div>
    );
}

interface AddCollectionFormData {
    collectionName: string;
}

interface FilterCollectionsFormData {
    filter: string;
}

// takes the current rows so the duplicate check is part of validation rather than a disabled button
const getAddCollectionSchema = (existingCollections: string[]) =>
    yup.object({
        collectionName: yup
            .string()
            .trim()
            .required("Enter a collection name")
            .test("not-duplicate", "This collection is already on the list", (value) =>
                existingCollections.every((name) => name.toLowerCase() !== value?.toLowerCase())
            ),
    });

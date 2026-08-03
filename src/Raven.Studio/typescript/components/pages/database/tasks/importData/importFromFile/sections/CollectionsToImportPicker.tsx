import React, { useState } from "react";
import { useForm, useFormContext, useWatch } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import { Icon } from "components/common/Icon";
import { FormInput } from "components/common/Form";
import { ImportFromFileFormData } from "../importFromFileValidation";

/**
 * Collections to import, typed in by hand: the server cannot list a dump file's collections before
 * the upload, so there is nothing to pick from. Two separate inputs on purpose - one adds a name,
 * the other only narrows the rows already added.
 */
export default function CollectionsToImportPicker() {
    const { control: importControl, setValue } = useFormContext<ImportFromFileFormData>();

    const includedCollections = useWatch({ control: importControl, name: "collections.includedCollections" }) ?? [];

    // Rows live in local state so deselecting only turns the toggle off - the trash icon removes
    // the row entirely.
    const [manualCollections, setManualCollections] = useState<string[]>([]);

    // A form of its own, separate from the page's import form: its only field is the name being
    // typed, which is never submitted to the server - it just appends a row. Keeping it local means
    // the duplicate/blank rules live in a schema and surface as a field error.
    const addCollectionForm = useForm({
        resolver: yupResolver(getAddCollectionSchema(manualCollections)),
        defaultValues: { collectionName: "" },
    });
    const {
        control: addControl,
        handleSubmit,
        setValue: setAddValue,
        clearErrors,
        setFocus,
    } = addCollectionForm;

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
        // setValue rather than reset: it clears the field without unregistering it, so the input
        // keeps its ref and setFocus below lands. Validating here would immediately complain the
        // field is empty, so a stale message from an earlier attempt is dropped explicitly instead.
        setAddValue("collectionName", "");
        clearErrors("collectionName");
        // keep the caret in the field so several collections can be typed in a row
        setFocus("collectionName");
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
            {/* FormInput renders its validation message as a sibling of the input wrapper, so the
                row itself is a flex container of just those two children - the message then lands
                below the row instead of stretching it and shrinking the button */}
            <Form onSubmit={handleSubmit(addCollection)} className="mb-3">
                <div className="d-flex gap-2">
                    <FormInput
                        type="text"
                        control={addControl}
                        name="collectionName"
                        placeholder="Type a collection name from the imported file"
                    />
                    {/* deliberately always enabled: handleSubmit blocks the append and the schema
                        message explains why, which beats a greyed-out button with no reason */}
                    <Button type="submit" variant="secondary" className="align-self-stretch">
                        <Icon icon="plus" margin="m-0" /> Add
                    </Button>
                </div>
            </Form>
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
                    <Form.Check
                        type="switch"
                        id="select-all-collections"
                        label=""
                        className="m-0"
                        disabled={filteredCollections.length === 0}
                        checked={areAllFilteredSelected}
                        onChange={toggleAllFiltered}
                    />
                </div>
            </div>
            <div className="import-collections-list">
                {filteredCollections.length === 0 && (
                    <div className="import-list-item text-muted">{getEmptyListMessage()}</div>
                )}
                {filteredCollections.map((name) => (
                    <div key={name} className="import-list-item">
                        <Form.Check
                            type="switch"
                            label={name}
                            checked={includedCollections.includes(name)}
                            onChange={(e) => toggleCollection(name, e.target.checked)}
                        />
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

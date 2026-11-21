import React, { ClipboardEvent, useEffect, useRef, useState } from "react";
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

export default function CollectionsToImportPicker() {
    const { control: importControl, setValue, getValues } = useFormContext<ImportFromFileFormData>();

    const includedCollections = useWatch({ control: importControl, name: "collections.includedCollections" }) ?? [];

    const [manualCollections, setManualCollections] = useState<string[]>(
        () => getValues("collections.includedCollections") ?? []
    );
    const latestManualCollections = useRef(manualCollections);
    latestManualCollections.current = manualCollections;
    const [addedCount, setAddedCount] = useState(0);

    const addCollectionForm = useForm({
        resolver: yupResolver(addCollectionSchema),
        defaultValues: { collectionName: "" },
    });
    const {
        control: addControl,
        handleSubmit,
        setValue: setAddValue,
        setError,
        clearErrors,
        setFocus,
    } = addCollectionForm;

    const areAllSelected =
        manualCollections.length > 0 && manualCollections.every((name) => includedCollections.includes(name));

    const setIncludedCollections = (names: string[]) =>
        setValue("collections.includedCollections", names, { shouldDirty: true });

    const addCollections = ({ collectionName }: AddCollectionFormData) => {
        const existing = latestManualCollections.current;
        const names = parseCollectionNames(collectionName);
        const added = names.filter((name) => !containsIgnoreCase(existing, name));
        const duplicates = names.filter((name) => containsIgnoreCase(existing, name));

        latestManualCollections.current = [...existing, ...added];
        setManualCollections(latestManualCollections.current);
        setIncludedCollections([...(getValues("collections.includedCollections") ?? []), ...added]);
        setAddedCount((prev) => prev + 1);
        setAddValue("collectionName", duplicates.join(", "));
        clearErrors("collectionName");

        if (duplicates.length > 0) {
            setError("collectionName", { message: getAlreadyOnListMessage(duplicates) });
        }
    };

    const pasteLinesAsCommaList = (e: ClipboardEvent<HTMLInputElement>) => {
        const pasted = e.clipboardData.getData("text");
        if (!/[\r\n]/.test(pasted)) {
            return;
        }

        e.preventDefault();
        const input = e.currentTarget;
        const before = input.value.slice(0, input.selectionStart ?? input.value.length);
        const after = input.value.slice(input.selectionEnd ?? input.value.length);

        setAddValue(
            "collectionName",
            [before, splitNames(pasted).join(", "), after].filter((x) => x.trim()).join(", ")
        );
    };

    // a disabled input ignores focus(), so wait until the add form has finished submitting
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

    const toggleAll = () => {
        setIncludedCollections(areAllSelected ? [] : [...manualCollections]);
    };

    return (
        <div className="mt-4">
            <InnerForm onSubmit={handleSubmit(addCollections)} className="mb-3">
                <InputGroup>
                    <FormInput
                        type="text"
                        control={addControl}
                        name="collectionName"
                        placeholder="Type or paste collection names, separated by commas"
                        aria-label="Collections to import"
                        onPaste={pasteLinesAsCommaList}
                    />
                    <Button variant="secondary" className="text-nowrap" onClick={handleSubmit(addCollections)}>
                        <Icon icon="plus" /> Add
                    </Button>
                </InputGroup>
            </InnerForm>
            <div className="import-list-header mb-2">
                <span className="flex-grow-1 fw-semibold">Collection name</span>
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
            </div>
            <div className="import-collections-list">
                {manualCollections.length === 0 && (
                    <div className="import-list-item text-muted">
                        No collections added. Type a collection name from the imported file above and click Add.
                    </div>
                )}
                {manualCollections.map((name) => (
                    <div key={name} className="import-list-item">
                        <Switch
                            id={`import-collection-${encodeURIComponent(name)}`}
                            color="primary"
                            selected={includedCollections.includes(name)}
                            toggleSelection={(e) => toggleCollection(name, e.target.checked)}
                            title={name}
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

const splitNames = (text: string) =>
    text
        .split(/[,\r\n]+/)
        .map((x) => x.trim())
        .filter(Boolean);

const containsIgnoreCase = (names: string[], name: string) => names.some((x) => x.toLowerCase() === name.toLowerCase());

const parseCollectionNames = (text: string) =>
    splitNames(text).reduce<string[]>(
        (unique, name) => (containsIgnoreCase(unique, name) ? unique : [...unique, name]),
        []
    );

const getAlreadyOnListMessage = (duplicates: string[]) => {
    const quoted = duplicates.map((x) => `"${x}"`).join(", ");
    const verb = duplicates.length === 1 ? "is" : "are";
    return `${quoted} ${verb} already on the list`;
};

const addCollectionSchema = yup.object({
    collectionName: yup
        .string()
        .test("required", "Enter a collection name", (value) => parseCollectionNames(value ?? "").length > 0),
});

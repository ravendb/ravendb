import React, { ClipboardEvent, ReactNode, useEffect, useRef, useState } from "react";
import { useForm, useFormState } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import Button from "react-bootstrap/Button";
import InputGroup from "react-bootstrap/InputGroup";
import { Icon } from "components/common/Icon";
import { FormInput } from "components/common/Form";
import InnerForm from "components/common/InnerForm";

interface CollectionNamesEditorProps {
    names: string[];
    onAdd: (added: string[]) => void;
    onRemove: (name: string) => void;
    inputLabel: string;
    listTitle: string;
    emptyText: string;
    headerAction?: ReactNode;
    renderName?: (name: string) => ReactNode;
}

export default function CollectionNamesEditor({
    names,
    onAdd,
    onRemove,
    inputLabel,
    listTitle,
    emptyText,
    headerAction,
    renderName = (name) => <span>{name}</span>,
}: CollectionNamesEditorProps) {
    const latestNames = useRef(names);
    latestNames.current = names;
    const [submitCount, setSubmitCount] = useState(0);

    const { control, handleSubmit, setValue, setError, clearErrors, setFocus } = useForm({
        resolver: yupResolver(schema),
        defaultValues: { collectionName: "" },
    });

    const addNames = ({ collectionName }: FormData) => {
        const parsed = parseCollectionNames(collectionName);
        const added = parsed.filter((name) => !containsIgnoreCase(latestNames.current, name));
        const duplicates = parsed.filter((name) => containsIgnoreCase(latestNames.current, name));

        latestNames.current = [...latestNames.current, ...added];
        if (added.length > 0) {
            onAdd(added);
        }

        setSubmitCount((prev) => prev + 1);
        setValue("collectionName", duplicates.join(", "));
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
        const pastedAsList = splitNames(pasted).join(", ");

        setValue("collectionName", [before, pastedAsList, after].filter((x) => x.trim()).join(", "));
    };

    // a disabled input ignores focus(), so wait until the add form has finished submitting
    const { isSubmitting } = useFormState({ control });

    useEffect(() => {
        if (isSubmitting) {
            return;
        }

        setFocus("collectionName");
    }, [submitCount, isSubmitting, setFocus]);

    return (
        <div className="mt-3">
            <InnerForm onSubmit={handleSubmit(addNames)} className="mb-3">
                <InputGroup>
                    <FormInput
                        type="text"
                        control={control}
                        name="collectionName"
                        placeholder="Type or paste collection names, separated by commas"
                        aria-label={inputLabel}
                        onPaste={pasteLinesAsCommaList}
                    />
                    <Button variant="secondary" className="text-nowrap" onClick={handleSubmit(addNames)}>
                        <Icon icon="plus" /> Add
                    </Button>
                </InputGroup>
            </InnerForm>
            <div className="import-list-header mb-2">
                <span className="flex-grow-1 fw-semibold">{listTitle}</span>
                {headerAction}
            </div>
            <div className="import-collections-list">
                {names.length === 0 && <div className="import-list-item text-muted">{emptyText}</div>}
                {names.map((name) => (
                    <div key={name} className="import-list-item">
                        {renderName(name)}
                        <Button
                            variant="link"
                            size="sm"
                            className="p-0 text-danger"
                            title={`Remove ${name}`}
                            onClick={() => onRemove(name)}
                        >
                            <Icon icon="trash" margin="m-0" />
                        </Button>
                    </div>
                ))}
            </div>
        </div>
    );
}

interface FormData {
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

const schema = yup.object({
    collectionName: yup
        .string()
        .test("required", "Enter a collection name", (value) => parseCollectionNames(value ?? "").length > 0),
});

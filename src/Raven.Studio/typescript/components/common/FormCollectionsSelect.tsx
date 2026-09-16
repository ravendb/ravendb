import React, { ComponentProps, useState } from "react";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import InputGroup from "react-bootstrap/InputGroup";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { EmptySet } from "./EmptySet";
import Button from "react-bootstrap/Button";
import { FlexGrow } from "./FlexGrow";
import { FormRadioToggleWithIcon, FormSelect, FormSelectCreatable } from "./Form";
import { RadioToggleWithIconInputItem } from "./toggles/RadioToggle";
import { Icon } from "./Icon";
import { SelectOption } from "./select/Select";
import { FieldPath, FieldValues, Control } from "react-hook-form";
import { GroupBase } from "react-select";

interface FormCollectionsSelectProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: Control<TFieldValues>;
    allCollectionNames: string[];
    isAllCollectionsFormName: TName;
    isAllCollections: boolean;
    collectionsFormName: TName;
    collections: string[];
    setValue: (name: TName, collections: string[], options: { shouldDirty: boolean }) => void;
    customOptions?: SelectOption<string>[];
    isReadOnly?: boolean;
    isCreatable?: boolean;
    // Hide the built-in "Selected / All collections" toggle when the parent already provides that
    // choice (e.g. the import view's "Import all collections / Customize" scope buttons).
    hideAllToggle?: boolean;
    // Free-text-only mode: when the available collections can't be enumerated up front (e.g. importing
    // from a .ravendbdump file), the user can only type collection names. Drops the dropdown chevron
    // and the "Add all" button, and uses an "Enter collection name" placeholder.
    isFreeTextEntry?: boolean;
}

export default function FormCollectionsSelect<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormCollectionsSelectProps<TFieldValues, TName>
) {
    const {
        control,
        allCollectionNames,
        isAllCollectionsFormName,
        isAllCollections,
        collectionsFormName,
        collections,
        setValue,
        customOptions,
        isReadOnly,
        hideAllToggle,
        isFreeTextEntry,
    } = props;

    const isCreatable = props.isCreatable ?? true;

    // In free-text mode the select has no menu, so we track the typed value ourselves and commit it
    // on Enter or via the "Add" button.
    const [typedCollection, setTypedCollection] = useState("");

    const addTypedCollection = () => {
        const name = typedCollection.trim();
        if (!name || collections.includes(name)) {
            return;
        }
        setValue(collectionsFormName, [...collections, name], { shouldDirty: true });
        setTypedCollection("");
    };

    const removeCollection = (name: string) => {
        setValue(
            collectionsFormName,
            collections.filter((x) => x !== name),
            { shouldDirty: true }
        );
    };

    const removeAllCollections = () => {
        setValue(collectionsFormName, [], { shouldDirty: true });
    };

    const addAllCollections = () => {
        const remainingCollectionNames = allCollectionNames.filter((name) => !collections.includes(name));
        setValue(collectionsFormName, [...collections, ...remainingCollectionNames], { shouldDirty: true });
    };

    const isAddAllCollectionsDisabled = allCollectionNames.filter((name) => !collections.includes(name)).length === 0;

    const formSelectProps: ComponentProps<
        typeof FormSelect<SelectOption, true, GroupBase<SelectOption>, TFieldValues, TName>
    > = {
        control: control,
        name: collectionsFormName,
        options: allCollectionNames.map((x) => ({ label: x, value: x })),
        isMulti: true,
        controlShouldRenderValue: false,
        isClearable: false,
        placeholder: "Select collection" + (isCreatable ? " (or enter new collection)" : ""),
        maxMenuHeight: 300,
    };

    // TODO Fix 'Add all' button height, when validation error occurs.

    return (
        <div className="vstack gap-2">
            {!isReadOnly && !hideAllToggle && (
                <div className="w-fit-content mx-auto">
                    <FormRadioToggleWithIcon
                        control={control}
                        name={isAllCollectionsFormName}
                        leftItem={leftRadioToggleItem}
                        rightItem={rightRadioToggleItem}
                        className="d-flex justify-content-center"
                    />
                </div>
            )}
            <Collapse in={!isAllCollections}>
                <div>
                    {!isReadOnly &&
                        (isFreeTextEntry ? (
                            // The available collections can't be enumerated (e.g. a .ravendbdump file), so
                            // this is a plain text input, not a select. InputGroup pairs it with the Add
                            // button at a matching height (the app's standard "input + button" pattern).
                            <InputGroup className="mb-3">
                                <Form.Control
                                    type="text"
                                    placeholder="Enter collection name"
                                    value={typedCollection}
                                    onChange={(e) => setTypedCollection(e.target.value)}
                                    onKeyDown={(e) => {
                                        if (e.key === "Enter" && typedCollection.trim()) {
                                            e.preventDefault();
                                            addTypedCollection();
                                        }
                                    }}
                                />
                                <Button
                                    variant="secondary"
                                    className="text-nowrap"
                                    onClick={addTypedCollection}
                                    disabled={!typedCollection.trim()}
                                >
                                    <Icon icon="plus" /> Add
                                </Button>
                            </InputGroup>
                        ) : (
                            <Row className="mb-4">
                                <Col>
                                    {isCreatable ? (
                                        <FormSelectCreatable {...formSelectProps} customOptions={customOptions} />
                                    ) : (
                                        <FormSelect {...formSelectProps} />
                                    )}
                                </Col>
                                <Col sm="auto" className="d-flex">
                                    <Button
                                        variant="info"
                                        onClick={addAllCollections}
                                        disabled={isAddAllCollectionsDisabled}
                                    >
                                        <Icon icon="documents" addon="plus" /> Add all
                                    </Button>
                                </Col>
                            </Row>
                        ))}
                    <div className="d-flex flex-wrap align-items-center mb-1">
                        {isFreeTextEntry ? (
                            <div className="m-0">Selected collections</div>
                        ) : (
                            <h4 className="m-0">Selected collections</h4>
                        )}
                        <FlexGrow />
                        {collections.length > 0 && !isReadOnly && (
                            <Button variant="link" size="xs" onClick={removeAllCollections} className="p-0">
                                Remove all
                            </Button>
                        )}
                    </div>
                    <div className="well p-2">
                        <div className="simple-item-list">
                            {collections.map((name) => (
                                <div key={name} className="p-1 hstack slidein-style">
                                    <div className="flex-grow-1">{name}</div>
                                    {!isReadOnly && (
                                        <Button
                                            variant="link"
                                            size="xs"
                                            onClick={() => removeCollection(name)}
                                            className="p-0"
                                        >
                                            <Icon icon="trash" margin="m-0" />
                                        </Button>
                                    )}
                                </div>
                            ))}
                        </div>
                        <Collapse in={collections.length === 0}>
                            <div>
                                <EmptySet>No collections have been selected</EmptySet>
                            </div>
                        </Collapse>
                    </div>
                </div>
            </Collapse>{" "}
        </div>
    );
}

const leftRadioToggleItem: RadioToggleWithIconInputItem<boolean> = {
    label: "Selected collections",
    value: false,
    iconName: "document",
};

const rightRadioToggleItem: RadioToggleWithIconInputItem<boolean> = {
    label: "All collections",
    value: true,
    iconName: "documents",
};

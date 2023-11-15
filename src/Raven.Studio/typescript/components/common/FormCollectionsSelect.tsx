import React from "react";
import { collectionsTrackerSelectors } from "./shell/collectionsTrackerSlice";
import { useAppSelector } from "components/store";
import { Collapse, Row, Col, Button } from "reactstrap";
import { EmptySet } from "./EmptySet";
import { FlexGrow } from "./FlexGrow";
import { FormRadioToggleWithIcon, FormSelectCreatable } from "./Form";
import { RadioToggleWithIconInputItem } from "./RadioToggle";
import { Icon } from "./Icon";
import { SelectOption } from "./select/Select";
import { FieldPath, FieldValues, Control } from "react-hook-form";

interface FormCollectionsSelectProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: Control<TFieldValues>;
    isAllCollectionsFormName: TName;
    isAllCollections: boolean;
    collectionsFormName: TName;
    collections: string[];
    setValue: (name: TName, collections: string[], options: { shouldDirty: boolean }) => void;
    customOptions?: SelectOption<string>[];
    isReadOnly?: boolean;
}

export default function FormCollectionsSelect<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>(
    props: FormCollectionsSelectProps<TFieldValues, TName>
) {
    const {
        control,
        isAllCollectionsFormName,
        isAllCollections,
        collectionsFormName,
        collections,
        setValue,
        customOptions,
        isReadOnly,
    } = props;

    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames).filter(
        (x) => x !== "@empty" && x !== "@hilo"
    );

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

    return (
        <div className="vstack gap-2">
            {!isReadOnly && (
                <FormRadioToggleWithIcon
                    control={control}
                    name={isAllCollectionsFormName}
                    leftItem={leftRadioToggleItem}
                    rightItem={rightRadioToggleItem}
                    className="d-flex justify-content-center"
                />
            )}
            <Collapse isOpen={!isAllCollections}>
                {!isReadOnly && (
                    <Row className="mb-4">
                        <Col>
                            <FormSelectCreatable
                                control={control}
                                name={collectionsFormName}
                                options={allCollectionNames.map((x) => ({ label: x, value: x }))}
                                customOptions={customOptions}
                                isMulti
                                controlShouldRenderValue={false}
                                isClearable={false}
                                placeholder="Select collection (or enter new collection)"
                                maxMenuHeight={300}
                            />
                        </Col>
                        <Col sm="auto" className="d-flex">
                            <Button color="info" onClick={addAllCollections} disabled={isAddAllCollectionsDisabled}>
                                <Icon icon="documents" addon="plus" /> Add all
                            </Button>
                        </Col>
                    </Row>
                )}
                <div className="d-flex flex-wrap mb-1 align-items-center">
                    <h4 className="m-0">Selected collections</h4>
                    <FlexGrow />
                    {collections.length > 0 && !isReadOnly && (
                        <Button color="link" size="xs" onClick={removeAllCollections} className="p-0">
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
                                        color="link"
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
                    <Collapse isOpen={collections.length === 0}>
                        <EmptySet>No collections have been selected</EmptySet>
                    </Collapse>
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

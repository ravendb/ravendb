import React from "react";
import { Button } from "reactstrap";
import copyToClipboard from "common/copyToClipboard";
import { Icon } from "components/common/Icon";
import { EmptySet } from "components/common/EmptySet";
import { FormInput } from "components/common/Form";
import { useForm } from "react-hook-form";

interface UnusedIdsSelectProps {
    ids: { vector: string }[];
    onRemoveId: (vector: string) => void;
}
export default function UnusedIdsSelect(props: UnusedIdsSelectProps) {
    const { ids, onRemoveId } = props;
    const { control } = useForm<null>({});
    return (
        <>
            <div className="d-flex gap-3 my-2">
                <FormInput
                    control={control}
                    name="newUnusedDatabaseId"
                    type="text"
                    placeholder="Enter database ID to add"
                />
                <Button color="primary" title="Add ID to the list">
                    <Icon icon="plus" /> Add ID
                </Button>
            </div>
            <div className="well p-2">
                <div className="simple-item-list">
                    {ids.length > 0 ? (
                        ids.map((id) => (
                            <div key={id.vector} className="p-1 hstack slidein-style">
                                <div className="flex-grow-1">{id.vector}</div>
                                <div className="d-flex gap-1">
                                    <Button
                                        color="link"
                                        size="xs"
                                        onClick={() =>
                                            copyToClipboard.copy(id.vector, `Copied ${id.vector} vector to clipboard`)
                                        }
                                        className="p-0"
                                        title="Copy to clipboard"
                                    >
                                        <Icon icon="copy-to-clipboard" margin="m-0" />
                                    </Button>
                                    <Button
                                        color="link"
                                        size="xs"
                                        onClick={() => onRemoveId(id.vector)}
                                        className="p-0"
                                        title="Remove"
                                    >
                                        <Icon icon="trash" margin="m-0" />
                                    </Button>
                                </div>
                            </div>
                        ))
                    ) : (
                        <EmptySet>No Unused IDs have been added</EmptySet>
                    )}
                </div>
            </div>
        </>
    );
}

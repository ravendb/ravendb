import { yupResolver } from "@hookform/resolvers/yup";
import copyToClipboard from "common/copyToClipboard";
import { EmptySet } from "components/common/EmptySet";
import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import React from "react";
import { useForm, SubmitHandler } from "react-hook-form";
import { Form, Button } from "reactstrap";
import * as yup from "yup";

interface UnusedIdsFormProps {
    ids: string[];
    addId: (id: string) => boolean;
    removeId: (id: string) => void;
}

export default function UnusedIdsForm(props: UnusedIdsFormProps) {
    const { ids, addId, removeId } = props;
    const { control, handleSubmit, reset } = useForm<FormData>({
        resolver: formResolver,
        defaultValues: { id: "" },
    });

    const onAddId: SubmitHandler<FormData> = ({ id }) => {
        const isAdded = addId(id);
        if (isAdded) {
            reset();
        }
    };

    return (
        <Form onSubmit={handleSubmit(onAddId)}>
            <div className="d-flex gap-3 my-2">
                <FormInput control={control} name="id" type="text" placeholder="Enter database ID to add" />
                <Button type="submit" color="primary" title="Add ID to the list">
                    <Icon icon="plus" /> Add ID
                </Button>
            </div>
            <div className="well p-2">
                <div className="simple-item-list">
                    {ids.length === 0 ? (
                        <EmptySet>No Unused IDs have been added</EmptySet>
                    ) : (
                        ids.map((id) => (
                            <div key={id} className="p-1 hstack slidein-style">
                                <div className="flex-grow-1">{id}</div>
                                <div className="d-flex gap-1">
                                    <Button
                                        color="link"
                                        size="xs"
                                        onClick={() => copyToClipboard.copy(id, `Copied ${id} vector to clipboard`)}
                                        className="p-0"
                                        title="Copy to clipboard"
                                    >
                                        <Icon icon="copy-to-clipboard" margin="m-0" />
                                    </Button>
                                    <Button
                                        color="link"
                                        size="xs"
                                        onClick={() => removeId(id)}
                                        className="p-0"
                                        title="Remove"
                                    >
                                        <Icon icon="trash" margin="m-0" />
                                    </Button>
                                </div>
                            </div>
                        ))
                    )}
                </div>
            </div>
        </Form>
    );
}

const schema = yup.object({
    id: yup.string().required().trim().strict().length(22),
});

type FormData = yup.InferType<typeof schema>;
const formResolver = yupResolver(schema);

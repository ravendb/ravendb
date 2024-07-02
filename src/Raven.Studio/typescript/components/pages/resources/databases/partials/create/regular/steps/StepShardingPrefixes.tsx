import React, { useEffect } from "react";
import { CreateDatabaseRegularFormData as FormData } from "../createDatabaseRegularValidation";
import { FieldArrayWithId, useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { Alert, Button, Table, UncontrolledTooltip } from "reactstrap";
import { Icon } from "components/common/Icon";
import { FormInput } from "components/common/Form";
import { Checkbox } from "components/common/Checkbox";

export default function StepShardingPrefixes() {
    const { control, trigger } = useFormContext<FormData>();

    const {
        shardingPrefixesStep: { prefixesForShards },
        replicationAndShardingStep: { shardsCount },
    } = useWatch({ control });

    const { fields, append, remove, update } = useFieldArray({
        control,
        name: "shardingPrefixesStep.prefixesForShards",
    });

    const toggleShard = (index: number, shardNumber: number) => {
        const field = prefixesForShards[index];

        const updatedShardNumbers = field.shardNumbers.includes(shardNumber)
            ? field.shardNumbers.filter((x) => x !== shardNumber)
            : [...field.shardNumbers, shardNumber];

        update(index, {
            prefix: field.prefix,
            shardNumbers: updatedShardNumbers,
        });
        trigger(`shardingPrefixesStep.prefixesForShards.${index}.shardNumbers`);
    };

    const allShardNumbers = [...new Array(shardsCount).keys()];

    return (
        <div className="text-center">
            <h2 className="text-center">Sharding Prefixes</h2>

            <Table responsive>
                <thead>
                    <tr>
                        <th></th>
                        {allShardNumbers.map((shardNumber) => (
                            <th key={shardNumber} className="px-0">
                                <Icon icon="shard" color="shard" />
                                {shardNumber}
                            </th>
                        ))}
                        <th></th>
                        <th className="px-0"></th>
                    </tr>
                </thead>
                <tbody>
                    {fields.map((field, index) => (
                        <PrefixRow
                            key={field.id}
                            field={field}
                            index={index}
                            allShardNumbers={allShardNumbers}
                            toggleShard={toggleShard}
                            remove={remove}
                        />
                    ))}
                </tbody>
            </Table>

            <Button
                type="button"
                color="shard"
                outline
                className="rounded-pill mt-2"
                onClick={() => append({ prefix: "", shardNumbers: [] })}
            >
                <Icon icon="plus" />
                Add prefix
            </Button>

            <div className="d-flex justify-content-center mt-3">
                <Alert color="warning">
                    Sharding prefixes can be defined only when creating a database, and cannot be modified.
                    <br />
                    Make sure everything is set up correctly.
                </Alert>
            </div>
        </div>
    );
}

interface PrefixRowProps {
    index: number;
    field: FieldArrayWithId<FormData, "shardingPrefixesStep.prefixesForShards", "id">;
    allShardNumbers: number[];
    toggleShard: (index: number, shardNumber: number) => void;
    remove: (index: number) => void;
}

function PrefixRow({ index, field, allShardNumbers, toggleShard, remove }: PrefixRowProps) {
    const { control, formState, trigger } = useFormContext<FormData>();

    const {
        shardingPrefixesStep: { prefixesForShards },
    } = useWatch({ control });

    const prefix = prefixesForShards[index].prefix;

    // Trigger validation for all fields when prefix changes (check for duplicates)
    useEffect(() => {
        if (!prefix) {
            return;
        }

        trigger("shardingPrefixesStep.prefixesForShards");
    }, [prefix, trigger]);

    const shardsError = formState.errors.shardingPrefixesStep?.prefixesForShards?.[index]?.shardNumbers?.message;

    return (
        <tr>
            <td>
                <FormInput
                    type="text"
                    control={control}
                    name={`shardingPrefixesStep.prefixesForShards.${index}.prefix`}
                    className="form-control"
                    placeholder="Prefix"
                    style={{ minWidth: "100px", maxWidth: "300px" }}
                />
            </td>
            {allShardNumbers.map((shardNumber) => (
                <td key={shardNumber} className="px-0 align-middle">
                    <Checkbox
                        size="lg"
                        selected={field.shardNumbers.includes(shardNumber)}
                        toggleSelection={() => toggleShard(index, shardNumber)}
                    />
                </td>
            ))}
            <td className="px-0 align-middle">
                {index !== 0 && (
                    <Button type="button" color="danger" outline onClick={() => remove(index)}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                )}
            </td>
            <td id={"prefixShardsError" + index} className="px-0 align-middle">
                {shardsError && (
                    <>
                        <Icon icon="warning" color="danger" margin="m-0" />
                        <UncontrolledTooltip target={"prefixShardsError" + index} placement="left">
                            {shardsError}
                        </UncontrolledTooltip>
                    </>
                )}
            </td>
        </tr>
    );
}

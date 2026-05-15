import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormGroup, FormSelect, FormSwitch } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { SelectOption } from "components/common/select/Select";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { FieldValues, FieldPath, useWatch, UseControllerProps } from "react-hook-form";

interface FormTaskResponsibleNodeProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: UseControllerProps<TFieldValues>["control"];
    isSetName: TName;
    nodeName: TName;
    isPinName: TName;
}

export function FormTaskResponsibleNode<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    control,
    isSetName,
    nodeName,
    isPinName,
}: FormTaskResponsibleNodeProps<TFieldValues, TName>) {
    const isSet = useWatch({ control, name: isSetName });
    const responsibleNode = useWatch({ control, name: nodeName });
    const isPin = useWatch({ control, name: isPinName });

    const isDatabaseSharded = useAppSelector(databaseSelectors.activeDatabase)?.isSharded ?? false;
    const allNodes = useAppSelector(clusterSelectors.allNodes);

    const possibleMentorOptions: SelectOption[] = allNodes
        .filter((x) => x.type === "Member")
        .map((x) => ({ value: x.nodeTag, label: `Node ${x.nodeTag}` }));

    return (
        <FormGroup>
            {possibleMentorOptions.length === 0 && (
                <RichAlert variant="warning">
                    Currently, the responsible node cannot be selected because there are no nodes available.
                </RichAlert>
            )}
            <FormGroup>
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: isDatabaseSharded,
                            message: "This option is not respected in case of sharded databases.",
                        },
                    ]}
                >
                    <FormSwitch control={control} name={isSetName} disabled={isDatabaseSharded}>
                        Set Responsible Node
                    </FormSwitch>
                </ConditionalPopover>
            </FormGroup>
            {isSet && (
                <>
                    <FormGroup>
                        <FormSelect control={control} name={nodeName} options={possibleMentorOptions} />
                    </FormGroup>
                    {responsibleNode && (
                        <FormGroup>
                            <FormSwitch control={control} name={isPinName} title="Toggle on to pin selected node">
                                Pin node
                            </FormSwitch>
                            <RichAlert variant="info" className="mt-2">
                                {isPin ? (
                                    <>
                                        The selected node is now Pinned to handle this task.
                                        <br />
                                        When this node is down, the task will Not execute as no other node will be
                                        selected to handle the task.
                                        <br />
                                        In case the node is removed from the Database Group, a failover will occur as
                                        the cluster will select another node to handle the task.
                                    </>
                                ) : (
                                    <>
                                        The selected node will be the Preferred Node to handle the task.
                                        <br />
                                        When this node is down, the cluster selects another node from the Database Group
                                        to handle the task.
                                    </>
                                )}
                            </RichAlert>
                        </FormGroup>
                    )}
                </>
            )}
        </FormGroup>
    );
}

import { Icon } from "components/common/Icon";
import { CreateDatabaseRegularFormData } from "../createDatabaseRegularValidation";
import React, { useEffect } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Table, Label } from "reactstrap";
import { FormSelect } from "components/common/Form";
import { OptionWithIcon, SelectOptionWithIcon, SingleValueWithIcon } from "components/common/select/Select";
import { Checkbox } from "components/common/Checkbox";
import { NodeSet, NodeSetLabel, NodeSetList, NodeSetItem } from "components/common/NodeSet";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { todo } from "common/developmentHelper";

todo("Feature", "Damian", "Add Auto fill button");

export default function CreateDatabaseRegularStepNodeSelection() {
    const { control, setValue, formState } = useFormContext<CreateDatabaseRegularFormData>();
    const formValues = useWatch({
        control,
    });

    // const availableNodes = useAppSelector(clusterSelectors.allNodes);
    // TODO show node url?
    const availableNodeTags = useAppSelector(clusterSelectors.allNodeTags);

    // const allActionContexts = ActionContextUtils.getContexts(nodeList);
    // const [selectedActionContexts, setSelectedActionContexts] = useState<DatabaseActionContexts[]>(allActionContexts);

    const nodeOptions: SelectOptionWithIcon[] = [
        {
            label: "None",
            value: "null",
        },
        ...availableNodeTags.map(
            (x) =>
                ({
                    label: x,
                    value: x,
                    icon: "node",
                    iconColor: "node",
                }) satisfies SelectOptionWithIcon
        ),
    ];

    const toggleNodeTag = (nodeTag: string) => {
        if (formValues.manualNodes.includes(nodeTag)) {
            setValue(
                "manualNodes",
                formValues.manualNodes.filter((x) => x !== nodeTag)
            );
        } else {
            setValue("manualNodes", [...formValues.manualNodes, nodeTag]);
        }
    };

    const isSelectAllNodesIndeterminate =
        formValues.manualNodes.length > 0 && formValues.manualNodes.length < availableNodeTags.length;

    const isSelectedAllNodes = formValues.manualNodes.length === availableNodeTags.length;

    const toggleAllNodeTags = () => {
        if (formValues.manualNodes.length === 0) {
            setValue("manualNodes", availableNodeTags);
        } else {
            setValue("manualNodes", []);
        }
    };

    useEffect(() => {
        if (!formValues.isSharded) {
            setValue("replicationFactor", formValues.manualNodes.length);
        }
    }, [formValues.isSharded, formValues.manualNodes, setValue]);

    const shardNumbers = new Array(formValues.shardsCount).fill(0).map((_, i) => i);
    const replicationNumbers = new Array(formValues.replicationFactor).fill(0).map((_, i) => i);

    return (
        <div className="text-center">
            <h2 className="text-center">Manual Node Selection</h2>

            {formValues.isSharded && (
                <>
                    {/* TODO @damian
                    <div className="text-end">
                        <Button type="button" color="info" size="sm" outline className="rounded-pill mb-2">
                            Auto fill
                        </Button>
                    </div> */}

                    <Table bordered>
                        <thead>
                            <tr>
                                {formValues.shardsCount > 1 && <th />}
                                {replicationNumbers.map((replicationNumber) => (
                                    <th key={replicationNumber}>
                                        Replica <strong>{replicationNumber + 1}</strong>
                                    </th>
                                ))}
                            </tr>
                        </thead>

                        <tbody>
                            {shardNumbers.map((shardNumber) => (
                                <tr key={shardNumber}>
                                    {formValues.shardsCount > 1 && (
                                        <th scope="row">
                                            <Icon icon="shard" color="shard" margin="m-0" /> {shardNumber}
                                        </th>
                                    )}

                                    {replicationNumbers.map((replicationNumber) => (
                                        <td key={`${shardNumber}-${replicationNumber}`} className="p-0">
                                            <FormSelect
                                                control={control}
                                                name={`manualShard.${shardNumber}.${replicationNumber}`}
                                                options={nodeOptions}
                                                isSearchable={false}
                                                components={{
                                                    Option: OptionWithIcon,
                                                    SingleValue: SingleValueWithIcon,
                                                }}
                                            ></FormSelect>
                                        </td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </Table>
                </>
            )}

            <div id="DropdownContainer"></div>

            <h3 className="mb-1">{formValues.isSharded ? "Orchestrator nodes" : "Available nodes"}</h3>
            <div className="mb-2">
                <small>minimum 1</small>
            </div>
            <NodeSet>
                <NodeSetLabel>
                    <Checkbox
                        size="lg"
                        toggleSelection={toggleAllNodeTags}
                        indeterminate={isSelectAllNodesIndeterminate}
                        selected={isSelectedAllNodes}
                        title="Select all or none"
                    />
                </NodeSetLabel>
                <NodeSetList>
                    {availableNodeTags.map((nodeTag) => (
                        <NodeSetItem key={nodeTag}>
                            <Label title={"Node " + nodeTag}>
                                <Icon icon="node" color="node" />
                                {nodeTag}
                                <div className="d-flex justify-content-center">
                                    <Checkbox
                                        toggleSelection={() => toggleNodeTag(nodeTag)}
                                        selected={formValues.manualNodes.includes(nodeTag)}
                                    />
                                </div>
                            </Label>
                        </NodeSetItem>
                    ))}
                </NodeSetList>
                {formState.errors.manualNodes && (
                    <div className="badge bg-danger rounded-pill margin-top-xxs">
                        {formState.errors.manualNodes.message}
                    </div>
                )}
            </NodeSet>
        </div>
    );
}

// interface NodeSelectionDropdownProps {
//     nodeList: string[];
//     id: string;
//     destinationNode: string;
//     handleUpdate: () => void;
// }

// function NodeSelectionDropdown(props: NodeSelectionDropdownProps) {
//     const { nodeList, destinationNode, handleUpdate } = props;
//     return (
//         <>
//             <UncontrolledDropdown>
//                 <DropdownToggle caret color="link" className="w-100" size="sm">
//                     {destinationNode == null ? (
//                         <>select</>
//                     ) : (
//                         <>
//                             <Icon icon="node" color="node" margin="m-0" /> {destinationNode}
//                         </>
//                     )}
//                 </DropdownToggle>
//                 <DropdownMenu container="DropdownContainer">
//                     {nodeList.map((nodeTag) => (
//                         <DropdownItem key={nodeTag} onClick={() => handleUpdate()}>
//                             <Icon icon="node" color="node" margin="m-0" /> {nodeTag}
//                         </DropdownItem>
//                     ))}
//                     <DropdownItem>
//                         <Icon icon="disabled" margin="m-0" /> None
//                     </DropdownItem>
//                 </DropdownMenu>
//             </UncontrolledDropdown>
//         </>
//     );
// }

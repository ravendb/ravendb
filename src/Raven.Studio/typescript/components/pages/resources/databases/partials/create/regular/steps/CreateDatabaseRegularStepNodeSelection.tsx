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

    // TODO use just watch?
    const manualNodes = formValues.manualNodeSelection.nodes;
    const isSharded = formValues.replicationAndSharding.isSharded;
    const shardsCount = formValues.replicationAndSharding.shardsCount;
    const replicationFactor = formValues.replicationAndSharding.replicationFactor;

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
        if (manualNodes.includes(nodeTag)) {
            setValue(
                "manualNodeSelection.nodes",
                manualNodes.filter((x) => x !== nodeTag)
            );
        } else {
            setValue("manualNodeSelection.nodes", [...manualNodes, nodeTag]);
        }
    };

    const isSelectAllNodesIndeterminate = manualNodes.length > 0 && manualNodes.length < availableNodeTags.length;

    const isSelectedAllNodes = manualNodes.length === availableNodeTags.length;

    const toggleAllNodeTags = () => {
        if (manualNodes.length === 0) {
            setValue("manualNodeSelection.nodes", availableNodeTags);
        } else {
            setValue("manualNodeSelection.nodes", []);
        }
    };

    useEffect(() => {
        if (!isSharded) {
            setValue("replicationAndSharding.replicationFactor", manualNodes.length);
        }
    }, [isSharded, manualNodes.length, setValue]);

    const shardNumbers = new Array(shardsCount).fill(0).map((_, i) => i);
    const replicationNumbers = new Array(replicationFactor).fill(0).map((_, i) => i);

    return (
        <div className="text-center">
            <h2 className="text-center">Manual Node Selection</h2>

            {isSharded && (
                <Table bordered>
                    <thead>
                        <tr>
                            {shardsCount > 1 && <th />}
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
                                {shardsCount > 1 && (
                                    <th scope="row">
                                        <Icon icon="shard" color="shard" margin="m-0" /> {shardNumber}
                                    </th>
                                )}

                                {replicationNumbers.map((replicationNumber) => (
                                    <td key={`${shardNumber}-${replicationNumber}`} className="p-0">
                                        <FormSelect
                                            control={control}
                                            name={`manualNodeSelection.shards.${shardNumber}.${replicationNumber}`}
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
            )}
            <h3 className="mb-1">{isSharded ? "Orchestrator nodes" : "Available nodes"}</h3>
            <div className="mb-2">
                <small>minimum 1</small>
            </div>
            <div>
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
                                            selected={manualNodes.includes(nodeTag)}
                                        />
                                    </div>
                                </Label>
                            </NodeSetItem>
                        ))}
                    </NodeSetList>
                </NodeSet>
            </div>
            {formState.errors.manualNodeSelection?.nodes && (
                <div className="badge bg-danger rounded-pill margin-top-xxs">
                    {formState.errors.manualNodeSelection.nodes.message}
                </div>
            )}
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

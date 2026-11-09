import Dropdown from "react-bootstrap/Dropdown";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useAppSelector } from "components/store";

export const allNodesOption = "all";

interface BackupDecisionLogNodeSelectorProps {
    selectedNodeTag: string | typeof allNodesOption;
    onChange: (value: string | typeof allNodesOption) => void;
}

export default function BackupDecisionLogNodeSelector({
    selectedNodeTag,
    onChange,
}: BackupDecisionLogNodeSelectorProps) {
    const allNodeTags = useAppSelector(clusterSelectors.allNodeTags);
    const isAllNodes = selectedNodeTag === allNodesOption;

    return (
        <Dropdown>
            <PopoverWithHoverWrapper
                message={
                    isAllNodes
                        ? "Shows decisions from all nodes in the cluster."
                        : `Shows decisions made by node ${selectedNodeTag} only. Each node keeps its own log.`
                }
            >
                <Dropdown.Toggle as={CustomDropdownToggle} variant="secondary" size="sm" className="rounded-pill">
                    <Icon icon={isAllNodes ? "cluster" : "node"} />
                    {isAllNodes ? "All nodes" : `Node ${selectedNodeTag}`}
                </Dropdown.Toggle>
            </PopoverWithHoverWrapper>
            <Dropdown.Menu>
                {allNodeTags.map((nodeTag) => (
                    <Dropdown.Item key={nodeTag} active={nodeTag === selectedNodeTag} onClick={() => onChange(nodeTag)}>
                        <Icon icon="node" />
                        Node {nodeTag}
                    </Dropdown.Item>
                ))}
                <Dropdown.Item active={isAllNodes} onClick={() => onChange(allNodesOption)}>
                    <Icon icon="cluster" />
                    All nodes
                </Dropdown.Item>
            </Dropdown.Menu>
        </Dropdown>
    );
}

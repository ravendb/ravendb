import React from "react";
import Select from "components/common/select/Select";
import { components, OptionProps, SingleValueProps } from "react-select";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import "./NodeLocationSelect.scss";

interface NodeLocationOption {
    value: number;
    label: string;
    nodeTag: string;
    shardNumber?: number;
}

function NodeLocationOptionComponent(props: OptionProps<NodeLocationOption>) {
    const { data } = props;
    return (
        <components.Option {...props}>
            <div className="d-flex align-items-center">
                <Icon icon="node" color="node" margin="m-0" />
                <span className="ms-1">{data.nodeTag}</span>
                {data.shardNumber != null && (
                    <>
                        <Icon icon="shard" color="shard" className="ms-2 me-0" margin="m-0" />
                        <span className="ms-1">#{data.shardNumber}</span>
                    </>
                )}
            </div>
        </components.Option>
    );
}

function NodeLocationSingleValueComponent(props: SingleValueProps<NodeLocationOption>) {
    const { data } = props;
    return (
        <components.SingleValue {...props}>
            <div className="d-flex align-items-center">
                <Icon icon="node" color="node" margin="m-0" />
                <span className="ms-1">{data.nodeTag}</span>
                {data.shardNumber != null && (
                    <>
                        <Icon icon="shard" color="shard" className="ms-2 me-0" margin="m-0" />
                        <span className="ms-1">#{data.shardNumber}</span>
                    </>
                )}
            </div>
        </components.SingleValue>
    );
}

interface NodeLocationSelectProps {
    locations: { nodeTag: string; shardNumber?: number }[];
    selectedIndex: number;
    onChange: (index: number) => void;
    className?: string;
}

export function NodeLocationTabs({ locations, selectedIndex, onChange, className }: NodeLocationSelectProps) {
    return (
        <div className={classNames("node-location-tabs", className)}>
            {locations.map((loc, i) => {
                const isActive = i === selectedIndex;
                return (
                    <button
                        key={i}
                        type="button"
                        className={classNames("node-location-tab", { active: isActive })}
                        onClick={() => onChange(i)}
                    >
                        <Icon icon="node" color="node" margin="m-0" />
                        <span>{loc.nodeTag}</span>
                        {loc.shardNumber != null && (
                            <>
                                <Icon icon="shard" color="shard" margin="m-0" />
                                <span>#{loc.shardNumber}</span>
                            </>
                        )}
                    </button>
                );
            })}
        </div>
    );
}

export function NodeLocationSelect({ locations, selectedIndex, onChange, className }: NodeLocationSelectProps) {
    const options: NodeLocationOption[] = locations.map((loc, i) => ({
        value: i,
        label: loc.shardNumber != null ? `${loc.nodeTag} #${loc.shardNumber}` : loc.nodeTag,
        nodeTag: loc.nodeTag,
        shardNumber: loc.shardNumber,
    }));

    return (
        <Select
            options={options}
            value={options[selectedIndex] ?? null}
            onChange={(opt) => onChange((opt as NodeLocationOption).value)}
            isSearchable={false}
            components={{
                Option: NodeLocationOptionComponent,
                SingleValue: NodeLocationSingleValueComponent,
            }}
            className={className}
            styles={{
                container: (base) => ({ ...base, maxWidth: "200px" }),
                control: (base) => ({ ...base, maxWidth: "200px" }),
            }}
        />
    );
}

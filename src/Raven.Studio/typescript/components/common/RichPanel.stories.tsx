import { ComponentMeta } from "@storybook/react";
import { boundCopy } from "../utils/common";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelSelect,
} from "./RichPanel";
import React, { useState } from "react";
import { Checkbox } from "./Checkbox";
import useBoolean from "hooks/useBoolean";
import { Button, Collapse } from "reactstrap";
import {
    DistributionItem,
    DistributionLegend,
    DistributionSummary,
    LocationDistribution,
} from "./LocationDistribution";
import { ProgressCircle } from "./ProgressCircle";
import { Icon } from "./Icon";

export default {
    title: "Bits/Rich Panel",
    decorators: [withStorybookContexts, withBootstrap5],
    component: RichPanel,
} as ComponentMeta<typeof RichPanel>;

const Template = (args: { withCheckbox: boolean }) => {
    const { value: checked, toggle: toggleCheckbox } = useBoolean(false);
    const { value: open, toggle: toggleOpen } = useBoolean(false);

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    {args.withCheckbox && (
                        <RichPanelSelect>
                            <Checkbox toggleSelection={toggleCheckbox} selected={checked} />
                        </RichPanelSelect>
                    )}
                    <RichPanelName>This is header</RichPanelName>
                </RichPanelInfo>
                <RichPanelActions>
                    <Button>Actions are placed here</Button>
                    <Button className="ms-1" color="shard" onClick={toggleOpen} outline={!open}>
                        <Icon icon="sharding" />
                    </Button>
                </RichPanelActions>
            </RichPanelHeader>

            <RichPanelDetails>
                <RichPanelDetailItem>
                    <Icon icon="check" className="me-1" />
                    Detail #1
                </RichPanelDetailItem>
                <RichPanelDetailItem>
                    <Icon icon="warning" className="me-1" /> Detail #2
                </RichPanelDetailItem>
                <RichPanelDetailItem size="sm">
                    <Icon icon="warning" className="me-1" /> Detail small
                </RichPanelDetailItem>

                <RichPanelDetailItem label="label">
                    <Icon icon="warning" className="me-1" /> Detail with label
                </RichPanelDetailItem>
                <RichPanelDetailItem
                    size="sm"
                    label={
                        <>
                            <Icon icon="processor" className="me-1" />
                            label with icon
                        </>
                    }
                >
                    Small with label
                </RichPanelDetailItem>
                <div className="rich-panel-details-right">
                    <RichPanelDetailItem label="Other">
                        <Icon icon="star" className="me-1" /> Detail placed right
                    </RichPanelDetailItem>
                </div>
            </RichPanelDetails>

            <Collapse isOpen={open}>
                <LocationDistribution>
                    <DistributionLegend>
                        <div className="top"></div>
                        <div className="node">
                            <Icon icon="node" className="me-1" /> Node
                        </div>
                        <div>
                            <Icon icon="list" className="me-1" /> Entries
                        </div>
                        <div>
                            <Icon icon="warning" className="me-1" /> Errors
                        </div>
                        <div>
                            <Icon icon="" className="me-1" />
                            Status
                        </div>
                    </DistributionLegend>

                    <DistributionSummary>
                        <div className="top">Total</div>
                        <div>A, B and C</div>
                        <div>Some total</div>
                        <div>16</div>
                        <div>OK</div>
                    </DistributionSummary>

                    <DistributionItem loading></DistributionItem>
                    <DistributionItem>
                        <div className="top node">
                            <Icon icon="node" className="me-1" /> A
                        </div>
                        <div>5</div>
                        <div>2</div>
                        <div>2</div>
                        <ProgressCircle state="success" icon="check">
                            up to date
                        </ProgressCircle>
                    </DistributionItem>
                    <DistributionItem>
                        <div className="top node">
                            <Icon icon="node" className="me-1" /> B
                        </div>
                        <div>5</div>
                        <div>2</div>
                        <div>2</div>
                        <ProgressCircle state="failed" icon="cancel">
                            error
                        </ProgressCircle>
                    </DistributionItem>
                </LocationDistribution>
            </Collapse>
        </RichPanel>
    );
};

export const Panel = boundCopy(Template);

Panel.args = {
    withCheckbox: false,
};

export const PanelWithCheckbox = boundCopy(Template);

PanelWithCheckbox.args = {
    withCheckbox: true,
};

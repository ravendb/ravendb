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
                        <i className="icon-sharding" />
                    </Button>
                </RichPanelActions>
            </RichPanelHeader>

            <RichPanelDetails>
                <RichPanelDetailItem>
                    <i className="icon-check" />
                    Detail #1
                </RichPanelDetailItem>
                <RichPanelDetailItem>
                    <i className="icon-warning" /> Detail #2
                </RichPanelDetailItem>
                <RichPanelDetailItem size="sm">
                    <i className="icon-warning" /> Detail small
                </RichPanelDetailItem>

                <RichPanelDetailItem label="label">
                    <i className="icon-warning" /> Detail with label
                </RichPanelDetailItem>
                <RichPanelDetailItem
                    size="sm"
                    label={
                        <>
                            <i className="icon-processor" />
                            label with icon
                        </>
                    }
                >
                    Small with label
                </RichPanelDetailItem>
                <div className="rich-panel-details-right">
                    <RichPanelDetailItem label="Other">
                        <i className="icon-star" /> Detail placed right
                    </RichPanelDetailItem>
                </div>
            </RichPanelDetails>

            <Collapse isOpen={open}>
                <LocationDistribution>
                    <DistributionLegend>
                        <div className="top"></div>
                        <div className="node">
                            <i className="icon-node" /> Node
                        </div>
                        <div>
                            <i className="icon-list" /> Entries
                        </div>
                        <div>
                            <i className="icon-warning" /> Errors
                        </div>
                        <div>
                            <i />
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
                            <i className="icon-node" /> A
                        </div>
                        <div>5</div>
                        <div>2</div>
                        <div>2</div>
                        <ProgressCircle state="success" icon="icon-check">
                            up to date
                        </ProgressCircle>
                    </DistributionItem>
                    <DistributionItem>
                        <div className="top node">
                            <i className="icon-node" /> B
                        </div>
                        <div>5</div>
                        <div>2</div>
                        <div>2</div>
                        <ProgressCircle state="failed" icon="icon-cancel">
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

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
} from "./RichPanel";
import React from "react";

import useBoolean from "hooks/useBoolean";
import { Button, Collapse } from "reactstrap";
import {
    DistributionItem,
    DistributionLegend,
    DistributionSummary,
    LocationDistribution,
} from "./LocationDistribution";
import { ProgressCircle } from "./ProgressCircle";
import { LazyLoad } from "./LazyLoad";
import { Icon } from "./Icon";

export default {
    title: "Bits/LazyLoad",
    decorators: [withStorybookContexts, withBootstrap5],
    component: LazyLoad,
} as ComponentMeta<typeof LazyLoad>;

const TemplatePanel = (args: { loadingActive: boolean }) => {
    const { value: open, toggle: toggleOpen } = useBoolean(false);

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
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
                <LazyLoad active={args.loadingActive}>
                    <RichPanelDetailItem>
                        <Icon icon="check" className="me-1" />
                        <span>Detail item #1</span>
                    </RichPanelDetailItem>
                </LazyLoad>

                <LazyLoad active={args.loadingActive}>
                    <RichPanelDetailItem>
                        <Icon icon="warning" className="me-1" /> Detail #2
                    </RichPanelDetailItem>
                </LazyLoad>
                <LazyLoad active={args.loadingActive}>
                    <RichPanelDetailItem size="sm">
                        <Icon icon="warning" className="me-1" /> <span>Detail small</span>
                    </RichPanelDetailItem>
                </LazyLoad>
                <LazyLoad active={args.loadingActive}>
                    <RichPanelDetailItem label="label">
                        <Icon icon="warning" className="me-1" /> <span>Detail with label</span>
                    </RichPanelDetailItem>
                </LazyLoad>
                <LazyLoad active={args.loadingActive}>
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
                </LazyLoad>
                <div className="rich-panel-details-right">
                    <LazyLoad active={args.loadingActive}>
                        <RichPanelDetailItem label="Other">
                            <Icon icon="star" className="me-1" /> Detail placed right
                        </RichPanelDetailItem>
                    </LazyLoad>
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

                    <DistributionItem loading={args.loadingActive}>
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

export const Panel = boundCopy(TemplatePanel);

Panel.args = {
    loadingActive: true,
};

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
                        <i className="icon-sharding" />
                    </Button>
                </RichPanelActions>
            </RichPanelHeader>

            <RichPanelDetails>
                <LazyLoad active={args.loadingActive}>
                    <RichPanelDetailItem>
                        <i className="icon-check" />
                        <span>Detail item #1</span>
                    </RichPanelDetailItem>
                </LazyLoad>

                <LazyLoad active={args.loadingActive}>
                    <RichPanelDetailItem>
                        <i className="icon-warning" /> Detail #2
                    </RichPanelDetailItem>
                </LazyLoad>
                <LazyLoad active={args.loadingActive}>
                    <RichPanelDetailItem size="sm">
                        <i className="icon-warning" /> <span>Detail small</span>
                    </RichPanelDetailItem>
                </LazyLoad>
                <LazyLoad active={args.loadingActive}>
                    <RichPanelDetailItem label="label">
                        <i className="icon-warning" /> <span>Detail with label</span>
                    </RichPanelDetailItem>
                </LazyLoad>
                <LazyLoad active={args.loadingActive}>
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
                </LazyLoad>
                <div className="rich-panel-details-right">
                    <LazyLoad active={args.loadingActive}>
                        <RichPanelDetailItem label="Other">
                            <i className="icon-star" /> Detail placed right
                        </RichPanelDetailItem>
                    </LazyLoad>
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

                    <DistributionItem loading={args.loadingActive}>
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

export const Panel = boundCopy(TemplatePanel);

Panel.args = {
    loadingActive: true,
};

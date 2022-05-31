import { ComponentMeta } from "@storybook/react";
import React from "react";
import {
    DistributionItem,
    DistributionLegend,
    DistributionSummary,
    LocationDistribution,
} from "./LocationDistribution";
import { ProgressCircle } from "./ProgressCircle";

export default {
    title: "Bits/Location Distribution",
    component: LocationDistribution,
} as ComponentMeta<typeof LocationDistribution>;

const Template = (args: { withSummary: boolean; withLegend: boolean }) => {
    return (
        <LocationDistribution>
            {args.withLegend && (
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
            )}
            {args.withSummary && (
                <DistributionSummary>
                    <div className="top">Total</div>
                    <div>A, B and C</div>
                    <div>Some total</div>
                    <div>16</div>
                    <div>OK</div>
                </DistributionSummary>
            )}
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
    );
};

export const WithSummary = Template.bind({});

WithSummary.args = {
    withSummary: true,
    withLegend: true,
};

export const WithOutSummary = Template.bind({});

WithOutSummary.args = {
    withSummary: false,
    withLegend: true,
};

export const TableOnly = Template.bind({});

TableOnly.args = {
    withSummary: false,
    withLegend: false,
};

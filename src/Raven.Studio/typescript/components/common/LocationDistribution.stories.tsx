import { ComponentMeta } from "@storybook/react";
import React from "react";
import {
    DistributionItem,
    DistributionLegend,
    DistributionSummary,
    LocationDistribution,
} from "./LocationDistribution";
import { ProgressCircle } from "./ProgressCircle";
import { boundCopy } from "../utils/common";
import { Icon } from "./Icon";

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
                        <Icon icon="node" /> Node
                    </div>
                    <div>
                        <Icon icon="list" /> Entries
                    </div>
                    <div>
                        <Icon icon="warning" /> Errors
                    </div>
                    <div>
                        <Icon icon="changes" /> State
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
                    <Icon icon="node" /> A
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
                    <Icon icon="node" /> B
                </div>
                <div>5</div>
                <div>2</div>
                <div>2</div>
                <ProgressCircle state="failed" icon="cancel">
                    error
                </ProgressCircle>
            </DistributionItem>
        </LocationDistribution>
    );
};

export const WithSummary = boundCopy(Template);

WithSummary.args = {
    withSummary: true,
    withLegend: true,
};

export const WithOutSummary = boundCopy(Template);

WithOutSummary.args = {
    withSummary: false,
    withLegend: true,
};

export const TableOnly = boundCopy(Template);

TableOnly.args = {
    withSummary: false,
    withLegend: false,
};

import { ComponentMeta } from "@storybook/react";
import { boundCopy } from "../utils/common";
import { RichPanel, RichPanelDetailItem, RichPanelDetails, RichPanelHeader, RichPanelSelect } from "./RichPanel";
import React, { useState } from "react";
import { Checkbox } from "./Checkbox";
import useBoolean from "hooks/useBoolean";

export default {
    title: "Bits/Rich Panel",
    component: RichPanel,
} as ComponentMeta<typeof RichPanel>;

const Template = (args: { withCheckbox: boolean }) => {
    const { value, toggle } = useBoolean(false);

    return (
        <RichPanel>
            <RichPanelHeader>
                {args.withCheckbox && (
                    <RichPanelSelect>
                        <Checkbox toggleSelection={toggle} selected={value} />
                    </RichPanelSelect>
                )}
                This is header
            </RichPanelHeader>
            <RichPanelDetails>
                <RichPanelDetailItem>
                    <i className="icon-check" />
                    Detail #1
                </RichPanelDetailItem>
                <RichPanelDetailItem>
                    <i className="icon-warning" /> Detail #2
                </RichPanelDetailItem>
            </RichPanelDetails>
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

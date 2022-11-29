import { ComponentMeta } from "@storybook/react";
import { boundCopy } from "../utils/common";
import { withBootstrap5, withStorybookContexts } from "../../test/storybookTestUtils";
import {
    RichPanel,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelName,
    RichPanelSelect,
} from "./RichPanel";
import React from "react";
import { Checkbox } from "./Checkbox";
import useBoolean from "hooks/useBoolean";

export default {
    title: "Bits/Rich Panel",
    decorators: [withStorybookContexts, withBootstrap5],
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
                <RichPanelName>This is header</RichPanelName>
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

                <RichPanelDetailItem label="Detail label">
                    <i className="icon-warning" /> Detail small
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
                    Detail small with label
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

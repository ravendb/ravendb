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
import React from "react";
import { Checkbox } from "./Checkbox";
import useBoolean from "hooks/useBoolean";
import { Button } from "reactstrap";

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
                <RichPanelInfo>
                    {args.withCheckbox && (
                        <RichPanelSelect>
                            <Checkbox toggleSelection={toggle} selected={value} />
                        </RichPanelSelect>
                    )}
                    <RichPanelName>This is header</RichPanelName>
                </RichPanelInfo>
                <RichPanelActions>
                    <Button>Actions are placed here</Button>
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

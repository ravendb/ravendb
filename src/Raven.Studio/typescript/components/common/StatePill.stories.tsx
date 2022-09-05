import { ComponentMeta } from "@storybook/react";
import React from "react";
import { StatePill } from "./StatePill";
import { boundCopy } from "../utils/common";

export default {
    title: "Bits/State Pills",
    component: StatePill,
} as ComponentMeta<typeof StatePill>;

const Template = () => {
    return (
        <div>
            <StatePill color="success">⭐ Success!</StatePill>
            <StatePill color="warning">‼️ Warning</StatePill>
            <StatePill color="danger">⚡ Danger </StatePill>
        </div>
    );
};

export const Pills = boundCopy(Template);

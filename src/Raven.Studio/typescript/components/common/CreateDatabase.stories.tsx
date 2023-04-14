import { ComponentMeta } from "@storybook/react";

import React, { useState } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Button, Card } from "reactstrap";
import { CreateDatabase } from "./CreateDatabase";
import { Icon } from "./Icon";
import { boundCopy } from "components/utils/common";

export default {
    title: "Bits/CreateDatabase",
    component: CreateDatabase,
    decorators: [withStorybookContexts, withBootstrap5],
    argTypes: {
        serverAuthentication: {
            control: {
                type: "boolean",
            },
        },
        createDatabaseModal: {
            control: "none",
        },
    },
} as ComponentMeta<typeof CreateDatabase>;

const TemplatePanel = (args: {
    serverAuthentication: boolean;
    licenseProps: { encryption: boolean; sharding: boolean; dynamicDatabaseDistribution: boolean };
}) => {
    const [createDatabaseModal, setCreateDatabaseModal] = useState(true);
    const toggleCreateDatabase = () => setCreateDatabaseModal(!createDatabaseModal);

    return (
        <>
            <Card className="p-4">
                <div>
                    <Button color="primary" onClick={toggleCreateDatabase}>
                        <Icon icon="database" addon="plus" className="me-1" /> New Database
                    </Button>
                </div>
            </Card>

            <CreateDatabase
                createDatabaseModal={createDatabaseModal}
                toggleCreateDatabase={toggleCreateDatabase}
                serverAuthentication={args.serverAuthentication}
                licenseProps={args.licenseProps}
            ></CreateDatabase>

            <div id="OverlayContainer"></div>
            <div id="PopoverContainer" className="popover-container-fix"></div>
        </>
    );
};

export const Panel = boundCopy(TemplatePanel, {
    serverAuthentication: true,
    licenseProps: {
        encryption: true,
        sharding: true,
        dynamicDatabaseDistribution: true,
    },
});

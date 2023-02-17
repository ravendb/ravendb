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
} as ComponentMeta<typeof CreateDatabase>;

const TemplatePanel = (args: { serverAuthentication: boolean }) => {
    const [createDatabaseModal, setCreateDatabaseModal] = useState(true);
    const toggleCreateDatabase = () => setCreateDatabaseModal(!createDatabaseModal);

    return (
        <>
            <Card className="p-4">
                <div>
                    <Button color="primary" onClick={toggleCreateDatabase}>
                        <Icon icon="database" addon="plus" /> New Database
                    </Button>
                </div>
            </Card>

            <CreateDatabase
                createDatabaseModal={createDatabaseModal}
                toggleCreateDatabase={toggleCreateDatabase}
                serverAuthentication={args.serverAuthentication}
            ></CreateDatabase>

            <div id="OverlayContainer"></div>
            <div id="PopoverContainer" className="popover-container-fix"></div>
        </>
    );
};

export const Panel = boundCopy(TemplatePanel, {
    serverAuthentication: true,
});

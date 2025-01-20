import { Meta, StoryObj } from "@storybook/react";
import React from "react";
import { databaseAccessArgType, withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import IndexErrors from "components/pages/database/indexes/errors/IndexErrors";
import { IndexesStubs } from "test/stubs/IndexesStubs";

export default {
    title: "Pages/Indexes/Index Errors",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

interface IndexErrorsStoryArgs {
    databaseAccess: databaseAccessLevel;
    isSharded: boolean;
    hasErrors: boolean;
}

const indexErrorsCountMock: { Results: indexErrorsCount[] } = {
    Results: [
        {
            Name: "Orders/ByShipment/Location",
            Errors: [],
        },
    ],
};

export const IndexErrorsStory: StoryObj<IndexErrorsStoryArgs> = {
    name: "Index Errors",
    render: (args) => {
        const { databases, accessManager } = mockStore;
        const { indexesService } = mockServices;

        let db;

        if (args.isSharded) {
            db = databases.withActiveDatabase_Sharded();
        } else {
            db = databases.withActiveDatabase_NonSharded_SingleNode();
        }

        accessManager.with_databaseAccess({
            [db.name]: args.databaseAccess,
        });

        indexesService.withGetIndexErrorsCount(
            args.hasErrors ? IndexesStubs.getIndexesErrorCount() : indexErrorsCountMock
        );
        indexesService.withGetIndexesErrorDetails(args.hasErrors ? IndexesStubs.getIndexErrorDetails() : []);

        return <IndexErrors />;
    },
    argTypes: {
        databaseAccess: databaseAccessArgType,
    },
    args: {
        databaseAccess: "DatabaseAdmin",
        isSharded: true,
        hasErrors: true,
    },
};

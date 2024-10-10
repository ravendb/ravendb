import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import AllRevisions from "./AllRevisions";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Documents/AllRevisions",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const AllRevisionsStory: StoryObj = {
    name: "All Revisions",
    render: () => {
        mockStore.databases.withActiveDatabase();
        mockServices.databasesService.withRevisionsPreview();

        return (
            <div style={{ height: "500px" }}>
                <AllRevisions />
            </div>
        );
    },
};

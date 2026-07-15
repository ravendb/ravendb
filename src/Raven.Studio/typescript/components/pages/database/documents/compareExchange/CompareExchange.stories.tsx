import { databaseAccessArgType, withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import CompareExchange from "components/pages/database/documents/compareExchange/CompareExchange";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Documents/Compare Exchange",
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/0rPQzkZhYPv4qKOGWtdNnZ/Pages---Compare-Exchange?node-id=1-7003",
        },
    },
} satisfies Meta<typeof CompareExchange>;

interface CompareExchangeStoryArgs {
    databaseAccess: databaseAccessLevel;
    itemsCount: number;
}

export const CompareExchangeStory: StoryObj<CompareExchangeStoryArgs> = {
    name: "Compare Exchange",
    render: (args) => {
        const { accessManager, databases } = mockStore;

        const { name } = databases.withActiveDatabase_NonSharded_SingleNode();
        accessManager.with_databaseAccess({
            [name]: args.databaseAccess,
        });

        const { databasesService } = mockServices;
        databasesService.withGetCompareExchangeItems(DatabasesStubs.compareExchangeItems(args.itemsCount));

        return (
            <div style={{ height: "800px" }}>
                <CompareExchange />
            </div>
        );
    },
    argTypes: {
        databaseAccess: databaseAccessArgType,
    },
    args: {
        databaseAccess: "DatabaseReadWrite",
        itemsCount: 30,
    },
};

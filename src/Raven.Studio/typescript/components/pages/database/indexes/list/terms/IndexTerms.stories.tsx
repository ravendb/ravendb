import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { IndexesStubs } from "test/stubs/IndexesStubs";
import TermsQueryResult = Raven.Client.Documents.Queries.TermsQueryResult;
import IndexTerms from "components/pages/database/indexes/list/terms/IndexTerms";

export default {
    title: "Pages/Indexes/Index Terms",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

interface IndexTermsStoryArgs {
    indexFieldsDto?: getIndexEntriesFieldsCommandResult;
    indexTerms?: TermsQueryResult;
    pathParams?: string[];
}

export const IndexTermsStory: StoryObj<IndexTermsStoryArgs> = {
    name: "Index Terms",
    render: (args) => {
        const { databases } = mockStore;
        databases.withActiveDatabase_NonSharded_SingleNode();
        const { indexesService } = mockServices;

        indexesService.withGetIndexFields(args.indexFieldsDto);
        indexesService.withGetIndexTerms(args.indexTerms);

        return <IndexTerms pathParams={args.pathParams} />;
    },
    args: {
        pathParams: ["Companies/StockPrices/TradeVolumeByMonth"],
        indexFieldsDto: IndexesStubs.getIndexTermFields(),
        indexTerms: IndexesStubs.getIndexTerms(),
    },
};

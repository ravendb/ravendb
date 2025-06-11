import { composeStories } from "@storybook/react";
import * as stories from "./IndexTerms.stories";
import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { IndexesStubs } from "test/stubs/IndexesStubs";
import { within } from "@testing-library/dom";
import { INDEX_TERMS_PAGE_LIMIT } from "components/pages/database/indexes/list/terms/useIndexTerms";
import TermsQueryResult = Raven.Client.Documents.Queries.TermsQueryResult;

const { IndexTermsStory } = composeStories(stories);

const pathParams = ["Orders/ByShipment/Location"];
const indexName = pathParams[0];

const testIdSelectors = {
    termAccordion: "term-accordion",
    termDynamicField: "term-dynamic-field",
    termVectorField: "term-vector-field",
    termPill: "term-pill",
    termLoadMoreButton: "term-load-more-btn",
};

type IndexTermsMockupType = Record<
    "indexFieldsDto" | "indexTerms",
    Record<string, TermsQueryResult | getIndexEntriesFieldsCommandResult[]>
>;

const indexTermsMockups: IndexTermsMockupType = {
    indexFieldsDto: {
        empty: [],
    },
    indexTerms: {
        empty: {
            IndexName: indexName,
            ResultEtag: -230492423094,
            Terms: [],
        },
        lessThanPageLimit: {
            IndexName: indexName,
            ResultEtag: -230492423094,
            Terms: Array.from<string>({ length: INDEX_TERMS_PAGE_LIMIT / 2 }).fill(new Date().toISOString()),
        },
    },
};

describe("IndexTerms", () => {
    it("can render indexName", async () => {
        const { screen } = rtlRender(<IndexTermsStory pathParams={pathParams} />);

        expect(await screen.findByText(indexName)).toBeInTheDocument();
    });

    it("can render accordion and number of accordion must equal number of static and dynamic fields", async () => {
        const { screen } = rtlRender(<IndexTermsStory pathParams={pathParams} />);

        const accordions = await screen.findAllByTestId(testIdSelectors.termAccordion);

        const expectedAccordionsLength =
            IndexesStubs.getIndexTermFields().filter((x) => x.FieldType === "Static").length +
            IndexesStubs.getIndexTermFields().filter((x) => x.FieldType === "Dynamic").length;

        expect(accordions).toHaveLength(expectedAccordionsLength);
    });

    it("can render accordion with 'dynamic field' badge", async () => {
        const { screen } = rtlRender(<IndexTermsStory pathParams={pathParams} />);

        const dynamicTermFields = await screen.findAllByTestId(testIdSelectors.termDynamicField);

        const termDynamicLength = IndexesStubs.getIndexTermFields().filter((x) => x.FieldType === "Dynamic").length;
        expect(dynamicTermFields).toHaveLength(termDynamicLength);
    });

    it("can render accordion with 'vector field' badge", async () => {
        const { screen } = rtlRender(<IndexTermsStory pathParams={pathParams} />);

        const dynamicTermFields = await screen.findAllByTestId(testIdSelectors.termVectorField);

        const termDynamicLength = IndexesStubs.getIndexTermFields().filter((x) => x.ValueType === "Vector").length;
        expect(dynamicTermFields).toHaveLength(termDynamicLength);
    });

    it("can render 'no fields were found' when fields arr = 0", async () => {
        const { screen } = rtlRender(
            <IndexTermsStory
                pathParams={pathParams}
                indexFieldsDto={indexTermsMockups.indexFieldsDto.empty as getIndexEntriesFieldsCommandResult[]}
            />
        );

        const accordions = screen.queryAllByTestId(testIdSelectors.termAccordion);
        const noFieldsText = await screen.findByText("No fields were found");

        expect(accordions).toHaveLength(0);
        expect(noFieldsText).toBeInTheDocument();
    });

    it("can render accordion with term pills", async () => {
        const { screen, user } = rtlRender(<IndexTermsStory pathParams={pathParams} />);

        const accordion = (await screen.findAllByTestId(testIdSelectors.termAccordion))[0];
        const accordionButton = within(accordion).getByRole("button");

        await user.click(accordionButton);

        const termPills = await within(accordion).findAllByTestId(testIdSelectors.termPill);

        expect(termPills).toHaveLength(INDEX_TERMS_PAGE_LIMIT);
    });

    it("can render text 'no more entries found' when terms length = 0", async () => {
        const { screen, user } = rtlRender(
            <IndexTermsStory
                indexTerms={indexTermsMockups.indexTerms.empty as TermsQueryResult}
                pathParams={pathParams}
            />
        );

        const accordion = (await screen.findAllByTestId(testIdSelectors.termAccordion))[0];
        const accordionButton = within(accordion).getByRole("button");

        await user.click(accordionButton);

        const termPills = within(accordion).queryAllByTestId(testIdSelectors.termPill);

        expect(termPills).toHaveLength((indexTermsMockups.indexTerms.empty as TermsQueryResult).Terms.length);

        const emptyEntriesText = within(accordion).queryByText("No entries were found.");

        expect(emptyEntriesText).toBeInTheDocument();
    });

    it("can render load more if terms array extend 500 length", async () => {
        const { screen, user } = rtlRender(<IndexTermsStory pathParams={pathParams} />);

        const accordion = (await screen.findAllByTestId(testIdSelectors.termAccordion))[0];
        const accordionButton = within(accordion).getByRole("button");

        await user.click(accordionButton);

        const loadMoreBtn = await within(accordion).findByTestId(testIdSelectors.termLoadMoreButton);

        expect(loadMoreBtn).toBeInTheDocument();
    });

    it("can not render load more if terms array not extend 500 length", async () => {
        const { screen, user } = rtlRender(
            <IndexTermsStory
                indexTerms={indexTermsMockups.indexTerms.empty as TermsQueryResult}
                pathParams={pathParams}
            />
        );

        const accordion = (await screen.findAllByTestId(testIdSelectors.termAccordion))[0];
        const accordionButton = within(accordion).getByRole("button");

        await user.click(accordionButton);

        const loadMoreBtn = within(accordion).queryByTestId(testIdSelectors.termLoadMoreButton);

        expect(loadMoreBtn).not.toBeInTheDocument();
    });
});

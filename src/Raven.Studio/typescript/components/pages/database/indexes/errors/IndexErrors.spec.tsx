import { rtlRender_WithWaitForLoad, waitFor } from "test/rtlTestUtils";
import { composeStories } from "@storybook/react-webpack5";
import * as stories from "./IndexErrors.stories";
import { within } from "@testing-library/dom";
import { IndexesStubs } from "test/stubs/IndexesStubs";
import { mockServices } from "test/mocks/services/MockServices";
import studioSettings from "common/settings/studioSettings";

const { IndexErrorsStory } = composeStories(stories);

const classSelectors = {
    nodePanel: "rich-panel-item",
    erroredNodePanelTotalErrorCountContainer: ".detail-item-content",
};

const textSelectors = {
    erroredNodePanelItemStatusBadge: "Errors",
    nodePanelItemStatusBadge: "OK",
    erroredNodePanelTotalErrorCount: "Total count",
    title: "Index Errors",
    clearErrorsButtonLabel: "Delete errors",
    confirmDeleteButtonLabel: "Delete",
    typedConfirmationLabel: "Type DELETE to confirm",
    requireTypedConfirmationLabel: "Require typed confirmation",
    relativeTimePattern: /ago$/,
    erroredDocumentId: IndexesStubs.getIndexErrorDetails()[2].Errors[0].Document,
};

const totalErrorCount = IndexesStubs.getIndexesErrorCount().Results.reduce(
    (count, item) => count + item.Errors.reduce((sum, error) => sum + error.NumberOfErrors, 0),
    0
);

describe("IndexErrors", function () {
    it("renders a single non-sharded node without errors", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IndexErrorsStory hasErrors={false} databaseAccess="DatabaseAdmin" isSharded={false} />
        );

        expect(await screen.findByRole("heading", { name: textSelectors.title })).toBeInTheDocument();
        expect(await screen.findByText(textSelectors.nodePanelItemStatusBadge)).toBeInTheDocument();
        expect(await screen.findAllByClassName(classSelectors.nodePanel)).toHaveLength(1);
    });

    it("renders sharded nodes without errors", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IndexErrorsStory hasErrors={false} databaseAccess="DatabaseAdmin" isSharded />
        );

        expect(await screen.findByRole("heading", { name: textSelectors.title })).toBeInTheDocument();
        expect(await screen.findAllByClassName(classSelectors.nodePanel)).toHaveLength(6);
    });

    it("renders a single non-sharded node with errors and displays total count", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IndexErrorsStory hasErrors databaseAccess="DatabaseAdmin" isSharded={false} />
        );

        expect(await screen.findByRole("heading", { name: textSelectors.title })).toBeInTheDocument();
        expect(await screen.findByText(textSelectors.erroredNodePanelItemStatusBadge)).toBeInTheDocument();

        const totalErrorsElement = (
            await screen.findByText(textSelectors.erroredNodePanelTotalErrorCount)
        ).closest<HTMLElement>(classSelectors.erroredNodePanelTotalErrorCountContainer);

        expect(within(totalErrorsElement).getByText(`${totalErrorCount} errors`)).toBeInTheDocument();
    });

    it("shows relative time inline in the Date cell without hovering", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IndexErrorsStory hasErrors databaseAccess="DatabaseAdmin" isSharded={false} />
        );

        expect(await screen.findByRole("heading", { name: textSelectors.title })).toBeInTheDocument();
        await screen.findAllByText(textSelectors.erroredDocumentId);

        expect(screen.getAllByText(textSelectors.relativeTimePattern).length).toBeGreaterThan(0);
    });

    it("does not show 'Delete errors' button for users with 'DatabaseRead' access", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IndexErrorsStory hasErrors databaseAccess="DatabaseRead" isSharded={false} />
        );

        expect(await screen.findByRole("heading", { name: textSelectors.title })).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: textSelectors.clearErrorsButtonLabel })).not.toBeInTheDocument();
    });

    it("renders a shard icon with isSharded is true", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IndexErrorsStory hasErrors databaseAccess="DatabaseRead" isSharded />
        );

        expect((await screen.findAllByClassName("icon-shard"))[0]).toBeInTheDocument();
    });

    it("does not render shard icon with isSharded is false", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <IndexErrorsStory hasErrors databaseAccess="DatabaseRead" isSharded={false} />
        );

        expect(screen.queryByClassName("icon-shard")).not.toBeInTheDocument();
    });

    describe("delete errors confirmation", () => {
        afterEach(async () => {
            const settings = await studioSettings.default.globalSettings();
            settings.isRequireTypedConfirmationToDeleteIndexErrors.setValueLazy(true);
        });

        it("keeps Delete disabled until DELETE is typed, then clears index errors", async () => {
            const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(
                <IndexErrorsStory hasErrors databaseAccess="DatabaseAdmin" isSharded={false} />
            );

            await fireClick(await screen.findByRole("button", { name: textSelectors.clearErrorsButtonLabel }));

            expect(await screen.findByText(textSelectors.typedConfirmationLabel)).toBeInTheDocument();

            const deleteButton = screen.getByRole("button", { name: textSelectors.confirmDeleteButtonLabel });
            expect(deleteButton).toBeDisabled();

            await fillInput(screen.getByPlaceholderText("DELETE"), "DELETE");
            expect(deleteButton).toBeEnabled();

            await fireClick(deleteButton);

            await waitFor(() =>
                expect(mockServices.indexesService.mock.clearIndexErrors).toHaveBeenCalledWith(
                    [],
                    expect.any(String),
                    expect.objectContaining({ nodeTag: expect.any(String) })
                )
            );
        });

        it("allows deleting without typing when typed confirmation is switched off", async () => {
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(
                <IndexErrorsStory hasErrors databaseAccess="DatabaseAdmin" isSharded={false} />
            );

            await fireClick(await screen.findByRole("button", { name: textSelectors.clearErrorsButtonLabel }));
            expect(await screen.findByText(textSelectors.typedConfirmationLabel)).toBeInTheDocument();

            const requireTypedConfirmationSwitch = screen.getByLabelText(textSelectors.requireTypedConfirmationLabel);
            await waitFor(() => expect(requireTypedConfirmationSwitch).toBeEnabled());

            await fireClick(requireTypedConfirmationSwitch);

            await waitFor(() =>
                expect(screen.queryByText(textSelectors.typedConfirmationLabel)).not.toBeInTheDocument()
            );
            expect(screen.getByRole("button", { name: textSelectors.confirmDeleteButtonLabel })).toBeEnabled();
        });
    });
});

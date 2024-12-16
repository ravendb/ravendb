import { rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import * as stories from "./DocumentRevisions.stories";
import { composeStories } from "@storybook/react";

const { DefaultDocumentRevisions } = composeStories(stories);

describe("DocumentRevisions", () => {
    it("can render for database admin", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <DefaultDocumentRevisions databaseAccess="DatabaseAdmin" licenseType="Enterprise" isCloud={false} />
        );

        expect(screen.queryByRole("button", { name: /Save/ })).toBeInTheDocument();
    });

    it("can render for access below database admin", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <DefaultDocumentRevisions databaseAccess="DatabaseRead" licenseType="Enterprise" isCloud={false} />
        );

        expect(screen.queryByRole("button", { name: /Save/ })).not.toBeInTheDocument();
    });

    it("can disable add button when CanSetupDefaultRevisionsConfiguration is false", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <DefaultDocumentRevisions canSetupDefaultRevisionsConfiguration={false} />
        );

        const addDefaultButton = screen.getAllByRole("button", { name: /Add new/ })[0];
        expect(addDefaultButton).toBeDisabled();
    });
});

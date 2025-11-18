import { composeStories } from "@storybook/react-webpack5";
import * as stories from "components/pages/database/settings/remoteAttachments/RemoteAttachments.stories";
import { rtlRender_WithWaitForLoad } from "test/rtlTestUtils";

const { DefaultRemoteAttachments } = composeStories(stories);

const selectors = {
    titles: {
        save: "Save",
        addNew: "Add new",
        enableRemoteAttachments: "Enable Remote Attachments",
    },
};

describe("RemoteAttachments", () => {
    it("shows the Save button for DatabaseAdmin access", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultRemoteAttachments databaseAccess="DatabaseAdmin" />);

        expect(screen.queryByRole("button", { name: selectors.titles.save })).toBeInTheDocument();
    });

    it("hides the Save button for access levels below DatabaseAdmin", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultRemoteAttachments databaseAccess="DatabaseRead" />);

        expect(screen.queryByRole("button", { name: selectors.titles.save })).not.toBeInTheDocument();
    });

    it("hides the Add new button when user cannot configure defaults (non-admin)", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultRemoteAttachments databaseAccess="DatabaseRead" />);

        const addDefaultButton = screen.queryByRole("button", { name: selectors.titles.addNew });
        expect(addDefaultButton).not.toBeInTheDocument();
    });

    it("enables the Add new button for admin when configuration is allowed", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultRemoteAttachments databaseAccess="DatabaseAdmin" />);

        const addDefaultButton = screen.getByRole("button", { name: selectors.titles.addNew });
        expect(addDefaultButton).not.toBeDisabled();
    });

    it("keeps Add new enabled after toggling Remote Attachments on", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(
            <DefaultRemoteAttachments hasRemoteAttachments={false} databaseAccess="DatabaseAdmin" />
        );

        const enableSwitch = screen.getByRole("checkbox", { name: selectors.titles.enableRemoteAttachments });
        const addButton = screen.getByRole("button", { name: selectors.titles.addNew });

        expect(enableSwitch).not.toBeChecked();

        expect(addButton).toBeDisabled();

        await fireClick(enableSwitch);

        expect(enableSwitch).toBeChecked();

        expect(addButton).toBeEnabled();
    });
});

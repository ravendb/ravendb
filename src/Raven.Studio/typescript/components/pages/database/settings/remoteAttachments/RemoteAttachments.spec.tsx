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
    it("shows the Save button enabled for DatabaseAdmin access", async () => {
        const { screen, fillInput } = await rtlRender_WithWaitForLoad(
            <DefaultRemoteAttachments databaseAccess="DatabaseAdmin" />
        );

        const saveButton = screen.queryByRole("button", { name: selectors.titles.save });
        const checkFrequencyInput = screen.getByName("checkFrequencyInSec");

        // Save button is disabled, if none of inputs are dirty
        expect(saveButton).toBeDisabled();

        await fillInput(checkFrequencyInput, "100");

        expect(saveButton).toBeInTheDocument();
        expect(saveButton).not.toBeDisabled();
    });

    it("shows the Save button but disables it for access levels below DatabaseAdmin", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultRemoteAttachments databaseAccess="DatabaseRead" />);

        const saveButton = screen.queryByRole("button", { name: selectors.titles.save });
        expect(saveButton).toBeInTheDocument();
        expect(saveButton).toBeDisabled();
    });

    it("shows the Add new button but disables it when user cannot configure defaults (non-admin)", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultRemoteAttachments databaseAccess="DatabaseRead" />);

        const addDefaultButton = screen.queryByRole("button", { name: selectors.titles.addNew });
        expect(addDefaultButton).toBeInTheDocument();
        expect(addDefaultButton).toBeDisabled();
    });

    it("enables the Add new button for admin when configuration is allowed", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultRemoteAttachments databaseAccess="DatabaseAdmin" />);

        const addDefaultButton = screen.getByRole("button", { name: selectors.titles.addNew });
        expect(addDefaultButton).toBeInTheDocument();
        expect(addDefaultButton).not.toBeDisabled();
    });

    it("keeps Add new enabled after toggling Remote Attachments on", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(
            <DefaultRemoteAttachments hasRemoteDestinations={false} databaseAccess="DatabaseAdmin" />
        );

        const enableSwitch = screen.getByRole("checkbox", { name: selectors.titles.enableRemoteAttachments });
        const addButton = screen.getByRole("button", { name: selectors.titles.addNew });

        expect(enableSwitch).not.toBeChecked();

        await fireClick(enableSwitch);

        expect(enableSwitch).toBeChecked();

        expect(addButton).toBeEnabled();
    });
});

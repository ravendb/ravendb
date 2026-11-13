import { rtlRender, waitFor } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import * as stories from "./StudioGlobalConfiguration.stories";
import { mockHooks } from "test/mocks/hooks/MockHooks";
import studioSettings = require("common/settings/studioSettings");
import globalSettings = require("common/settings/globalSettings");

const { StudioConfiguration, LicenseRestricted } = composeStories(stories);

describe("StudioGlobalConfiguration", function () {
    it("can render", async () => {
        const { screen } = rtlRender(<StudioConfiguration />);
        expect(await screen.findByText("Collapse documents when opening")).toBeInTheDocument();
    });

    it("is license restricted", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        expect(await screen.findByText(/Licensing/)).toBeInTheDocument();
    });

    describe("while the usage stats question is unanswered", () => {
        beforeEach(() => {
            jest.spyOn(Storage.prototype, "setObject").mockImplementation(function (
                this: Storage,
                key: string,
                value: unknown
            ) {
                this.setItem(key, JSON.stringify(value));
            });
            jest.spyOn(Storage.prototype, "getObject").mockImplementation(function (this: Storage, key: string) {
                return JSON.parse(this.getItem(key));
            });
        });

        afterEach(() => {
            jest.restoreAllMocks();
            localStorage.removeItem(globalSettings.storageKey);
        });

        it("is clean again after saving other settings", async () => {
            const view = rtlRender(<StudioConfiguration />);

            await turnOnAndSave(view, "Collapse documents when opening");

            expect(view.screen.getByRole("button", { name: "Save" })).toBeDisabled();
            expect(mockHooks.useDirtyFlag.isDirty).toBe(false);
        });

        it("leaves it unanswered after saving other settings", async () => {
            const view = rtlRender(<StudioConfiguration />);

            await turnOnAndSave(view, "Collapse documents when opening");

            const savedSettings = await studioSettings.default.globalSettings(true);
            expect(savedSettings.collapseDocsWhenOpening.getValue()).toBe(true);
            expect(savedSettings.sendUsageStats.getValue()).toBeUndefined();
        });

        it("saves the answer once the switch is turned on", async () => {
            const view = rtlRender(<StudioConfiguration />);

            await turnOnAndSave(view, "Help improve the Studio by gathering anonymous usage statistics");

            const savedSettings = await studioSettings.default.globalSettings(true);
            expect(savedSettings.sendUsageStats.getValue()).toBe(true);
        });
    });
});

async function turnOnAndSave({ screen, user }: ReturnType<typeof rtlRender>, label: string) {
    await user.click(await screen.findByLabelText(label));
    await waitFor(() => expect(mockHooks.useDirtyFlag.isDirty).toBe(true));

    await user.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(mockHooks.useDirtyFlag.isDirty).toBe(false));
}

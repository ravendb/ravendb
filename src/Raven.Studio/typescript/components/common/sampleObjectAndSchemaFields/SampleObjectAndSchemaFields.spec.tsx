import { composeStories } from "@storybook/react-webpack5";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./SampleObjectAndSchemaFields.stories";

const { WithoutStrictMode, StrictModeOn, GeneratedSchema, GeneratedSchemaWithSamplesPanel, Empty } =
    composeStories(stories);

const selectors = {
    jsonSchemaTab: "Parameters JSON schema",
    strictSwitch: "Strict mode",
    generatedSchemaStrictSwitch: "Strict mode (always on for a generated schema)",
    strictBadge: "Strict",
    sentInStrictMode: "This schema will be sent to the model in strict mode",
    sentWithoutStrictMode: "This schema will be sent to the model",
};

describe("SampleObjectAndSchemaFields", () => {
    it("renders the view schema button inside the JSON schema editor so the samples panel does not shift it", async () => {
        const { screen, fireClick } = rtlRender(<GeneratedSchemaWithSamplesPanel />);

        await fireClick(screen.getByText(selectors.jsonSchemaTab));

        expect(screen.getByRole("button", { name: /View schema/ }).closest(".ace-editor")).not.toBeNull();
    });

    it("shows no strict mode switch when the schema has no strict mode", () => {
        const { screen } = rtlRender(<WithoutStrictMode />);

        expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
        expect(screen.getByText(selectors.sentWithoutStrictMode)).toBeInTheDocument();
    });

    it("marks a written schema as strict until strict mode is turned off", async () => {
        const { screen, fireClick } = rtlRender(<StrictModeOn />);

        expect(screen.getByText(selectors.strictBadge)).toBeInTheDocument();
        expect(screen.getByText(selectors.sentInStrictMode)).toBeInTheDocument();

        await fireClick(screen.getByRole("checkbox", { name: selectors.strictSwitch }));

        expect(screen.queryByText(selectors.strictBadge)).not.toBeInTheDocument();
        expect(screen.getByText(selectors.sentWithoutStrictMode)).toBeInTheDocument();
    });

    it("keeps strict mode on for a generated schema and lets the user change it after viewing the schema", async () => {
        const { screen, fireClick } = rtlRender(<GeneratedSchema />);

        for (const lockedSwitch of screen.getAllByRole("checkbox", { name: selectors.generatedSchemaStrictSwitch })) {
            expect(lockedSwitch).toBeChecked();
            expect(lockedSwitch).toBeDisabled();
        }

        await fireClick(screen.getByRole("button", { name: /View schema/ }));

        const strictSwitch = await screen.findByRole("checkbox", { name: selectors.strictSwitch });
        expect(strictSwitch).toBeChecked();
        expect(strictSwitch).toBeEnabled();
    });

    it("shows strict mode as always on in both tabs before a sample object or a schema is written", () => {
        const { screen } = rtlRender(<Empty />);

        const lockedSwitches = screen.getAllByRole("checkbox", { name: selectors.generatedSchemaStrictSwitch });

        expect(lockedSwitches).toHaveLength(2);
        for (const lockedSwitch of lockedSwitches) {
            expect(lockedSwitch).toBeChecked();
            expect(lockedSwitch).toBeDisabled();
        }
    });
});

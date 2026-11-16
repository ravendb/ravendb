import { composeStories } from "@storybook/react-webpack5";
import { rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import { changeLanguage } from "common/i18n/i18n";
import * as stories from "./EditBackupTask.stories";

const { Default, NewPeriodicBackup } = composeStories(stories);

describe("EditBackupTask in Polish", () => {
    beforeAll(async () => {
        await changeLanguage("pl");
    });

    afterAll(async () => {
        await changeLanguage("en");
    });

    it("translates the wizard steps and the backup types", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<NewPeriodicBackup />);

        expect(screen.getByRole("heading", { level: 2, name: /Wybierz typ kopii zapasowej/ })).toBeInTheDocument();
        expect(screen.getByRole("radio", { name: "Migawka" })).toBeInTheDocument();
        expect(screen.getByText("Zaplanuj kopie zapasowe")).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Dalej" })).toBeInTheDocument();
    });

    it("translates the summary of an existing task", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<Default />);

        expect(screen.getByRole("region", { name: "Ustawienia konfiguracji" })).toBeInTheDocument();
        expect(screen.getByRole("region", { name: "Miejsca docelowe" })).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Zapisz" })).toBeInTheDocument();
    });
});

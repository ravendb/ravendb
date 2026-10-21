import { rtlRender } from "test/rtlTestUtils";
import Button from "react-bootstrap/Button";
import { useViewSheet, useViewSheetCloseConfirmation, ViewSheet } from "./ViewSheet";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { mockHooks } from "test/mocks/hooks/MockHooks";

function FormSheet({ isConfirmationRequired }: { isConfirmationRequired: boolean }) {
    useViewSheetCloseConfirmation(isConfirmationRequired);

    return (
        <ViewSheet>
            <span>Form sheet</span>
            <ViewSheet.CloseButton>Close sheet</ViewSheet.CloseButton>
        </ViewSheet>
    );
}

function SheetOpeners({
    isConfirmationRequired,
    isPageDirty = false,
}: {
    isConfirmationRequired: boolean;
    isPageDirty?: boolean;
}) {
    const { open } = useViewSheet();
    useDirtyFlag(isPageDirty);

    return (
        <>
            <Button onClick={() => open({ component: <FormSheet isConfirmationRequired={isConfirmationRequired} /> })}>
                Open form sheet
            </Button>
            <Button onClick={() => open({ component: <span>Other sheet</span> })}>Open other sheet</Button>
        </>
    );
}

async function renderWithOpenedFormSheet(isConfirmationRequired: boolean, isPageDirty = false) {
    const rendered = rtlRender(
        <SheetOpeners isConfirmationRequired={isConfirmationRequired} isPageDirty={isPageDirty} />
    );
    await rendered.fireClick(rendered.screen.getByText("Open form sheet"));
    return rendered;
}

describe("ViewSheet", () => {
    it("closes a sheet without unsaved changes immediately", async () => {
        const { screen, fireClick } = await renderWithOpenedFormSheet(false);

        await fireClick(screen.getByText("Close sheet"));

        expect(screen.queryByText("Unsaved changes")).not.toBeInTheDocument();
        expect(screen.queryByText("Form sheet")).not.toBeInTheDocument();
    });

    it("keeps a sheet with unsaved changes open when closing is cancelled", async () => {
        const { screen, fireClick } = await renderWithOpenedFormSheet(true);

        await fireClick(screen.getByText("Close sheet"));
        expect(screen.getByText("Unsaved changes")).toBeInTheDocument();

        await fireClick(screen.getByText("Cancel"));

        expect(screen.queryByText("Unsaved changes")).not.toBeInTheDocument();
        expect(screen.getByText("Form sheet")).toBeInTheDocument();
    });

    it("closes a sheet with unsaved changes once they are discarded", async () => {
        const { screen, fireClick } = await renderWithOpenedFormSheet(true);

        await fireClick(screen.getByText("Close sheet"));
        await fireClick(screen.getByText("Discard"));

        expect(screen.queryByText("Form sheet")).not.toBeInTheDocument();
    });

    it("asks before replacing a sheet with unsaved changes", async () => {
        const { screen, fireClick } = await renderWithOpenedFormSheet(true);

        await fireClick(screen.getByText("Open other sheet"));
        expect(screen.getByText("Unsaved changes")).toBeInTheDocument();

        await fireClick(screen.getByText("Discard"));

        expect(screen.queryByText("Form sheet")).not.toBeInTheDocument();
        expect(screen.getByText("Other sheet")).toBeInTheDocument();
    });

    it("marks the page dirty only while a sheet has unsaved changes", async () => {
        const { screen, fireClick } = await renderWithOpenedFormSheet(true);
        expect(mockHooks.useDirtyFlag.isDirty).toBe(true);

        await fireClick(screen.getByText("Close sheet"));
        await fireClick(screen.getByText("Discard"));

        expect(mockHooks.useDirtyFlag.isDirty).toBe(false);
    });

    it("keeps the page dirty when a sheet without unsaved changes closes", async () => {
        const { screen, fireClick } = await renderWithOpenedFormSheet(false, true);

        await fireClick(screen.getByText("Close sheet"));

        expect(mockHooks.useDirtyFlag.isDirty).toBe(true);
    });
});

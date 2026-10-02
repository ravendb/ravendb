import React from "react";
import { rtlChangeLanguage, rtlRender } from "test/rtlTestUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";

function SaveLabel() {
    const t = useStudioTranslation("documentRefresh");
    return <span>{t("common:save")}</span>;
}

function ResolvingLabel({ documentId }: { documentId: string }) {
    const t = useStudioTranslation("conflicts");
    return <span>{t("resolvingConflictFor", { documentId })}</span>;
}

describe("useStudioTranslation", () => {
    afterEach(async () => {
        await rtlChangeLanguage("en");
    });

    it("translates common keys through MockProviders", () => {
        const { screen } = rtlRender(<SaveLabel />);
        expect(screen.getByText("Save")).toBeInTheDocument();
    });

    it("re-renders on language change", async () => {
        const { screen } = rtlRender(<SaveLabel />);
        await rtlChangeLanguage("pl");
        expect(await screen.findByText("Zapisz")).toBeInTheDocument();
    });

    it("leaves escaping of interpolated values to React", () => {
        const { screen } = rtlRender(<ResolvingLabel documentId="<b>orders/1</b>" />);
        expect(screen.getByText("Resolving conflict for: <b>orders/1</b>")).toBeInTheDocument();
    });
});

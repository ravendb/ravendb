import React from "react";
import { rtlChangeLanguage, rtlRender } from "test/rtlTestUtils";
import { translate } from "common/i18n/i18n";
import { useStudioTranslation } from "hooks/useStudioTranslation";

function SaveLabel() {
    const { t } = useStudioTranslation("common");
    return <span>{t("save")}</span>;
}

function TypoLabel() {
    const { t } = useStudioTranslation("common");
    // @ts-expect-error unknown key must not type-check
    return <span>{t("doesNotExist")}</span>;
}

function OtherNamespaceLabel() {
    const { t } = useStudioTranslation("documentRefresh");
    // @ts-expect-error key from another namespace must not type-check
    return <span>{t("common:save")}</span>;
}

describe("i18n React integration", () => {
    afterEach(async () => {
        await rtlChangeLanguage("en");
    });

    it("translates through MockProviders", () => {
        const { screen } = rtlRender(<SaveLabel />);
        expect(screen.getByText("Save")).toBeInTheDocument();
    });

    it("re-renders on language change", async () => {
        const { screen } = rtlRender(<SaveLabel />);
        await rtlChangeLanguage("pl");
        expect(await screen.findByText("Zapisz")).toBeInTheDocument();
    });

    it("throws on missing key in tests", () => {
        expect(typeof TypoLabel).toBe("function");
        expect(typeof OtherNamespaceLabel).toBe("function");
        // @ts-expect-error unknown key must not type-check
        expect(() => translate("common:doesNotExist")).toThrow("Missing translation key");
    });
});

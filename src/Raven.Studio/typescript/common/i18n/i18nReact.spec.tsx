import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

function SaveLabel() {
    const t = useStudioTranslation("common");
    return <span>{t("save")}</span>;
}

function ResolvingLabel({ documentId }: { documentId: string }) {
    const t = useStudioTranslation("conflicts");
    return <span>{t("resolvingConflictFor", { documentId })}</span>;
}

describe("useStudioTranslation", () => {
    it("translates common keys through MockProviders", () => {
        const { screen } = rtlRender(<SaveLabel />);
        expect(screen.getByText("Save")).toBeInTheDocument();
    });

    it("leaves escaping of interpolated values to React", () => {
        const { screen } = rtlRender(<ResolvingLabel documentId="<b>orders/1</b>" />);
        expect(screen.getByText("Resolving conflict for: <b>orders/1</b>")).toBeInTheDocument();
    });
});

describe("StudioTrans", () => {
    it("passes context and renders interpolated values as text", () => {
        const { screen } = rtlRender(
            <>
                <p>
                    <StudioTrans ns="featureAvailabilitySummary" i18nKey="description" options={{ context: "quill" }} />
                </p>
                <p>
                    <StudioTrans
                        ns="conflicts"
                        i18nKey="resolvingConflictFor"
                        options={{ documentId: "<b>orders/1</b>" }}
                    />
                </p>
            </>
        );
        expect(screen.getByText("See what features are included in this license")).toBeInTheDocument();
        expect(screen.getByText("Resolving conflict for: <b>orders/1</b>")).toBeInTheDocument();
    });

    it("rejects at compile time what renders wrong at runtime", () => {
        const invalidElements = () => [
            // @ts-expect-error missing context for a key that has only context variants
            <StudioTrans key="context" ns="featureAvailabilitySummary" i18nKey="description" />,
            // @ts-expect-error missing interpolation options
            <StudioTrans key="options" ns="conflicts" i18nKey="resolvingConflictFor" />,
            // @ts-expect-error options for a key without placeholders
            <StudioTrans key="extra" ns="conflicts" i18nKey="noConflicts" options={{ documentId: "orders/1" }} />,
        ];
        expect(invalidElements).toBeDefined();
    });
});

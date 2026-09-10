import React, { useState } from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { I18nextProvider } from "react-i18next";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { i18n } from "common/i18n/i18n";
import { languageNames, StudioLanguage } from "common/i18n/resources";
import DocumentRefresh from "components/pages/database/settings/documentRefresh/DocumentRefresh";

export default {
    title: "Bits/i18n Demo",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

interface FixedLanguageProps {
    language: StudioLanguage;
    children: React.ReactNode;
}

function FixedLanguage({ language, children }: FixedLanguageProps) {
    const [instance] = useState(() => i18n.cloneInstance({ lng: language }));
    return <I18nextProvider i18n={instance}>{children}</I18nextProvider>;
}

export const SideBySide: StoryObj = {
    name: "Side by side (EN / PL)",
    render: () => {
        mockServices.databasesService.withRefreshConfiguration();
        mockStore.databases.withActiveDatabase_NonSharded_SingleNode();
        mockStore.license.with_License();

        return (
            <div className="d-flex gap-4 align-items-start">
                {(["en", "pl"] as StudioLanguage[]).map((language) => (
                    <div key={language} className="flex-grow-1" style={{ minWidth: 0 }}>
                        <h4 className="mb-3">{languageNames[language]}</h4>
                        <FixedLanguage language={language}>
                            <DocumentRefresh />
                        </FixedLanguage>
                    </div>
                ))}
            </div>
        );
    },
};

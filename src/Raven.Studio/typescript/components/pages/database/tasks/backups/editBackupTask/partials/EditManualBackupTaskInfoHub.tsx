import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { useEditBackupTaskFeatureAvailability } from "../hooks/useEditBackupTaskFeatureAvailability";

export function EditManualBackupTaskInfoHub() {
    const { featureAvailability, isUnlimited, isOpenedByDefault } = useEditBackupTaskFeatureAvailability(true);

    const backupsDocsLink = useRavenLink({ hash: "GMBYOH" });
    const t = useStudioTranslation("editBackupTask");
    const commonT = useStudioTranslation("common");

    return (
        <AboutViewFloating>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <div>
                    {t("infoHub.manualIntro")}
                    <ul className="margin-top-xxs">
                        <li>{t("infoHub.manualAdHocItem")}</li>
                        <li>{t("infoHub.manualRetentionItem")}</li>
                    </ul>
                </div>
                <div>
                    {t("infoHub.optionsIntro")}
                    <ul className="margin-top-xxs">
                        <li>{t("infoHub.typeItem")}</li>
                        <li>{t("infoHub.manualDestinationsItem")}</li>
                        <li>{t("infoHub.encryptionItem")}</li>
                    </ul>
                </div>
                <hr />
                <div className="small-label mb-2">{commonT("usefulLinks")}</div>
                <a href={backupsDocsLink} target="_blank">
                    <Icon icon="newtab" /> {t("infoHub.docsLink")}
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={isUnlimited}
                data={featureAvailability}
                isOpenedByDefault={isOpenedByDefault}
            />
        </AboutViewFloating>
    );
}

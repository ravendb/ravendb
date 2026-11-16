import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper from "components/common/FeatureAvailabilitySummary";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import { useEditBackupTaskFeatureAvailability } from "../hooks/useEditBackupTaskFeatureAvailability";

export function EditPeriodicBackupTaskInfoHub() {
    const hasPeriodicBackups = useAppSelector(licenseSelectors.statusValue("HasPeriodicBackup"));
    const { featureAvailability, isUnlimited, isOpenedByDefault } = useEditBackupTaskFeatureAvailability(false);

    const backupsDocsLink = useRavenLink({ hash: "GMBYOH" });
    const t = useStudioTranslation("editBackupTask");
    const commonT = useStudioTranslation("common");

    return (
        <AboutViewFloating defaultOpen={hasPeriodicBackups ? null : "licensing"}>
            <AccordionItemWrapper targetId="about" icon="about" color="info">
                <p>
                    <StudioTrans
                        ns="editBackupTask"
                        i18nKey="infoHub.periodicIntro"
                        components={{ strong: <strong /> }}
                    />
                </p>
                <div>
                    {t("infoHub.optionsIntro")}
                    <ul>
                        <li>{t("infoHub.typeItem")}</li>
                        <li>{t("infoHub.periodicScheduleItem")}</li>
                        <li>{t("infoHub.periodicRetentionItem")}</li>
                        <li>{t("infoHub.periodicDestinationsItem")}</li>
                        <li>{t("infoHub.encryptionItem")}</li>
                    </ul>
                </div>
                <div>
                    {t("infoHub.periodicInAddition")}
                    <ul>
                        <li>{t("infoHub.periodicStateItem")}</li>
                        <li>{t("infoHub.periodicNodeItem")}</li>
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

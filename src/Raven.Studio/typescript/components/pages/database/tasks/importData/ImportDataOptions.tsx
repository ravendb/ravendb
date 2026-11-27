import "./ImportDataOptions.scss";
import React from "react";
import { AboutViewHeading } from "components/common/AboutView";
import NavigationCard, { NavigationCardProps } from "components/common/navigationCard/NavigationCard";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

type ImportDataVariant = "ImportFile" | "ImportRavenDb" | "ImportCsv" | "ImportSql" | "ImportNoSql";

export default function ImportDataOptions() {
    const t = useStudioTranslation("importData");
    const { forCurrentDatabase } = useAppUrls();

    const importOptions: NavigationCardProps<ImportDataVariant>[] = [
        {
            title: t("options.file.title"),
            description: t("options.file.description"),
            iconName: "file-import",
            variant: "ImportFile",
            link: forCurrentDatabase.importDatabaseFromFileUrl(),
            target: "ImportFromFile",
            accessRequired: "DatabaseReadWrite",
            isShardingSupported: true,
        },
        {
            title: t("options.ravenDb.title"),
            description: t("options.ravenDb.description"),
            iconName: "server",
            variant: "ImportRavenDb",
            link: forCurrentDatabase.migrateRavenDbDatabaseUrl(),
            target: "ImportFromRavenDbServer",
            accessRequired: "DatabaseReadWrite",
            isShardingSupported: true,
        },
        {
            title: t("options.csv.title"),
            description: t("options.csv.description"),
            iconName: "csv-import",
            variant: "ImportCsv",
            link: forCurrentDatabase.importCollectionFromCsv(),
            target: "ImportFromCsv",
            accessRequired: "DatabaseReadWrite",
            isShardingSupported: true,
        },
        {
            title: t("options.sql.title"),
            description: t("options.sql.description"),
            iconName: "sql-etl",
            variant: "ImportSql",
            link: forCurrentDatabase.importDatabaseFromSql(),
            target: "ImportFromSql",
            accessRequired: "DatabaseReadWrite",
            isShardingSupported: true,
        },
        {
            title: t("options.noSql.title"),
            description: t("options.noSql.description"),
            iconName: "documents",
            variant: "ImportNoSql",
            link: forCurrentDatabase.migrateDatabaseUrl(),
            target: "ImportFromNoSql",
            accessRequired: "DatabaseReadWrite",
            isShardingSupported: true,
        },
    ];

    return (
        <div className="content-margin">
            <AboutViewHeading title={t("options.title")} icon="import-database" marginBottom={2} />
            <div className="text-muted mb-4">{t("options.description")}</div>
            <div className="d-grid gap-3 navigation-cards-grid">
                {importOptions.map((option) => (
                    <NavigationCard key={option.variant} {...option} />
                ))}
            </div>
        </div>
    );
}

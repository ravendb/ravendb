import "./ImportDataOptions.scss";
import React from "react";
import { AboutViewHeading } from "components/common/AboutView";
import NavigationCard, { NavigationCardProps } from "components/common/navigationCard/NavigationCard";
import { useAppUrls } from "hooks/useAppUrls";

// Keep in sync with the navigation-card-variant includes in ImportDataOptions.scss
type ImportDataVariant = "ImportFile" | "ImportRavenDb" | "ImportCsv" | "ImportSql" | "ImportNoSql";

export default function ImportDataOptions() {
    const { forCurrentDatabase } = useAppUrls();

    const importOptions: NavigationCardProps<ImportDataVariant>[] = [
        {
            title: "From file (.ravendbdump)",
            description:
                "Restore a full database, including documents, indexes, and settings, from a single .ravendbdump backup file.",
            iconName: "file-import",
            variant: "ImportFile",
            link: forCurrentDatabase.importDatabaseFromFileUrl(),
            target: "ImportFromFile",
            accessRequired: "DatabaseReadWrite",
            isShardingSupported: true,
        },
        {
            title: "From RavenDB Server",
            description:
                "Migrate chosen collections, documents, and indexes directly from another active RavenDB server instance.",
            iconName: "server",
            variant: "ImportRavenDb",
            link: forCurrentDatabase.migrateRavenDbDatabaseUrl(),
            target: "ImportFromRavenDbServer",
            accessRequired: "DatabaseReadWrite",
            isShardingSupported: true,
        },
        {
            title: "From CSV File",
            description:
                "Import documents into a new or existing collection by mapping columns from a CSV file to document properties.",
            iconName: "csv-import",
            variant: "ImportCsv",
            link: forCurrentDatabase.importCollectionFromCsv(),
            target: "ImportFromCsv",
            accessRequired: "DatabaseReadWrite",
            isShardingSupported: true,
        },
        {
            title: "From SQL",
            description:
                "Migrate data from a SQL database using a connection string and a custom script to transform tables into documents.",
            iconName: "sql-etl",
            variant: "ImportSql",
            link: forCurrentDatabase.importDatabaseFromSql(),
            target: "ImportFromSql",
            accessRequired: "DatabaseReadWrite",
            isShardingSupported: true,
        },
        {
            title: "From NoSQL",
            description:
                "Import documents from various NoSQL data sources, including MongoDB dumps or older RavenDB embedded files.",
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
            <AboutViewHeading title="Import data" icon="import-database" marginBottom={2} />
            <div className="text-muted mb-4">
                Choose an import option that best suits your needs and make use of premade import settings presets.
            </div>
            <div className="d-grid gap-3 navigation-cards-grid">
                {importOptions.map((option) => (
                    <NavigationCard key={option.title} {...option} />
                ))}
            </div>
        </div>
    );
}

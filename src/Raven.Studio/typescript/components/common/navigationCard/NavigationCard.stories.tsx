import "components/pages/database/tasks/ongoingTasks/AddNewOngoingTask.scss";
import "components/pages/database/tasks/importData/ImportDataOptions.scss";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import NavigationCard from "./NavigationCard";
import { mockStore } from "test/mocks/store/MockStore";
import IconName from "typings/server/icons";

export default {
    title: "Bits/Navigation Card",
    decorators: [withStorybookContexts, withBootstrap5],
    component: NavigationCard,
} satisfies Meta;

const variants: { variant: string; iconName: IconName }[] = [
    { variant: "AI", iconName: "ai-etl" },
    { variant: "Replication", iconName: "external-replication" },
    { variant: "Backups", iconName: "backups" },
    { variant: "Subscriptions", iconName: "subscription" },
    { variant: "ETL", iconName: "etl" },
    { variant: "Sink", iconName: "kafka-sink" },
    { variant: "ImportFile", iconName: "file-import" },
    { variant: "ImportRavenDb", iconName: "server" },
    { variant: "ImportCsv", iconName: "csv-import" },
    { variant: "ImportSql", iconName: "sql-etl" },
    { variant: "ImportNoSql", iconName: "documents" },
];

export const AllVariants: StoryObj = {
    render: () => {
        const { accessManager, databases } = mockStore;

        const db = databases.withActiveDatabase_NonSharded_SingleNode();

        accessManager.with_databaseAccess({
            [db.name]: "DatabaseAdmin",
        });

        accessManager.with_securityClearance("ValidUser");

        return (
            <div className="d-grid gap-3 navigation-cards-grid">
                {variants.map(({ variant, iconName }) => (
                    <NavigationCard
                        key={variant}
                        title={variant}
                        description={`Example card using the ${variant} variant.`}
                        iconName={iconName}
                        variant={variant}
                        link="#"
                        target="StorybookNavigationCard"
                        accessRequired="DatabaseReadWrite"
                        isShardingSupported
                    />
                ))}
            </div>
        );
    },
};

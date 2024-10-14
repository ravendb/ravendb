import { ColumnDef } from "@tanstack/react-table";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import document from "models/database/documents/document";

export const columnDocumentFlags: ColumnDef<document> = {
    id: "Flags",
    header: () => (
        <span>
            <Icon icon="flag" />
            Flags
        </span>
    ),
    accessorFn: (x) => x.__metadata,
    cell: ({ getValue }) => {
        const metadata = getValue<document["__metadata"]>();
        const flags = metadata.flags ?? "";

        return (
            <span className="flags">
                <Icon
                    icon="attachment"
                    title="Attachments"
                    className={classNames({ attachments: flags.includes("HasAttachments") })}
                />
                <Icon
                    icon="revisions"
                    title="Revisions"
                    className={classNames({ revisions: flags.includes("HasRevisions") })}
                />
                <Icon
                    icon="new-counter"
                    title="Counters"
                    className={classNames({ counters: flags.includes("HasCounters") })}
                />
                <Icon
                    icon="new-time-series"
                    title="Time Series"
                    className={classNames({ "time-series": flags.includes("HasTimeSeries") })}
                />
                <Icon icon="data-archival" title="Archived" className={classNames({ archived: metadata.archived })} />
                {metadata.shardNumber && (
                    <span title={`Shard ${metadata.shardNumber}`} className="small-label">
                        <Icon icon="shard" color="shard" />
                        {metadata.shardNumber}
                    </span>
                )}
            </span>
        );
    },
};

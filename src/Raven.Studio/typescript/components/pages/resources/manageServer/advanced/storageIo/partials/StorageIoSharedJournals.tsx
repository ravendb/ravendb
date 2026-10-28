import React from "react";
import Card from "react-bootstrap/Card";
import { Icon } from "components/common/Icon";
import { storageTermHelp } from "common/storageIoTerms";
import { formatAgo, formatBytes, formatMs } from "common/storageIoFormat";

interface StorageIoSharedJournalsProps {
    sharedJournals: Raven.Server.Storage.SharedJournalsIoInfo[];
}

export default function StorageIoSharedJournals({ sharedJournals }: StorageIoSharedJournalsProps) {
    if (sharedJournals.length === 0) {
        return <div className="text-muted small">No database with shared index journals is loaded.</div>;
    }

    return (
        <>
            {sharedJournals.map((shared) => {
                const fellBack = shared.IndexesNotSharing.filter((x) => x.Reason === "HardLinkLimit");
                const notLinked = shared.IndexesNotSharing.filter((x) => x.Reason === "NotLinked");
                const totalSize = shared.Files.reduce((sum, f) => sum + f.SizeInBytes, 0);

                return (
                    <Card key={shared.Database} className="mb-2">
                        <Card.Body className="py-2">
                            <div className="hstack gap-2 flex-wrap mb-1">
                                <strong>{shared.Database}</strong>
                                <span className="text-muted small" title={storageTermHelp.sharedJournal}>
                                    {shared.IndexesSharing} index(es) share {shared.Files.length} journal file(s),{" "}
                                    {formatBytes(totalSize)}
                                </span>
                            </div>
                            <div className="small">
                                Index commits waiting: <strong>{shared.MergedCommitsQueue}</strong>, commit to durable:{" "}
                                <strong>
                                    {shared.CommitToDurableInMs > 0 ? formatMs(shared.CommitToDurableInMs) : "idle"}
                                </strong>
                            </div>
                            {shared.RetainedBy && (
                                <div className="small">
                                    Oldest shared journal kept by index <strong>{shared.RetainedBy.Index}</strong>{" "}
                                    (journal {shared.RetainedBy.JournalNumber}
                                    {shared.RetainedBy.AlreadyReleasedBySharedJournal &&
                                        ", already released by the shared journal, only this index keeps its disk space"}
                                    )
                                </div>
                            )}
                            {shared.LinksOfUnknownSource > 0 && (
                                <div className="small text-muted">
                                    {shared.LinksOfUnknownSource} journal link(s) recovered at startup: their shared
                                    journal is known once they are recycled.
                                </div>
                            )}
                            {fellBack.length > 0 && (
                                <div className="small text-warning mt-1">
                                    <Icon icon="warning" />
                                    Writing their own journals (file system hard link limit reached):{" "}
                                    {fellBack.map((x) => `${x.Index} (${formatAgo(x.Since)})`).join(", ")}. They rejoin
                                    after a database restart.
                                </div>
                            )}
                            {notLinked.length > 0 && (
                                <div className="small text-muted mt-1">
                                    Not sharing (shared journals disabled, or the index is on another volume):{" "}
                                    {notLinked.map((x) => x.Index).join(", ")}
                                </div>
                            )}
                        </Card.Body>
                    </Card>
                );
            })}
        </>
    );
}

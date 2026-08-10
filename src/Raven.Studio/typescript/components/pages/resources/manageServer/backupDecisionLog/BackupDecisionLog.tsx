import React, { useMemo, useState } from "react";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Badge from "react-bootstrap/Badge";
import Form from "react-bootstrap/Form";
import { useAsync } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import useInterval from "components/hooks/useInterval";
import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Checkbox } from "components/common/Checkbox";
import { FlexGrow } from "components/common/FlexGrow";
import { HrHeader } from "components/common/HrHeader";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import BackupDecisionLogAboutView from "./partials/BackupDecisionLogAboutView";
import BackupDecisionLogSummary from "./partials/BackupDecisionLogSummary";
import BackupDecisionLogEntries from "./partials/BackupDecisionLogEntries";

type DecisionKind = Raven.Server.ServerWide.Backups.BackupDecisionKind;

const allDatabasesOption = "__all__";
const allKindsOption = "__all__";

const kindOptions: DecisionKind[] = ["Policy", "Started", "Completed", "Failed", "Cancelled", "Info"];

const autoRefreshIntervalInMs = 5_000;

export default function BackupDecisionLog() {
    const { manageServerService } = useServices();

    const [databaseFilter, setDatabaseFilter] = useState<string>(allDatabasesOption);
    const [kindFilter, setKindFilter] = useState<string>(allKindsOption);
    const [searchText, setSearchText] = useState<string>("");
    const [autoRefresh, setAutoRefresh] = useState<boolean>(false);

    const asyncGetDecisionLog = useAsync(
        () =>
            manageServerService.getBackupDecisionLog(
                databaseFilter === allDatabasesOption ? undefined : databaseFilter
            ),
        [databaseFilter]
    );

    useInterval(
        () => {
            if (asyncGetDecisionLog.loading === false) {
                asyncGetDecisionLog.execute();
            }
        },
        autoRefresh ? autoRefreshIntervalInMs : null
    );

    const result = asyncGetDecisionLog.result;

    const filteredEntries = useMemo(() => {
        if (!result) {
            return [];
        }

        const search = searchText.trim().toLowerCase();

        return result.Entries.filter((entry) => {
            if (kindFilter !== allKindsOption && entry.Kind !== kindFilter) {
                return false;
            }

            if (search && !entry.Reason?.toLowerCase().includes(search)) {
                return false;
            }

            return true;
        });
    }, [result, kindFilter, searchText]);

    if (asyncGetDecisionLog.status === "error") {
        return <LoadError error="Unable to load the backup decision log" refresh={asyncGetDecisionLog.execute} />;
    }

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <div className="d-flex align-items-start gap-2">
                            <AboutViewHeading title="Backup Decision Log" icon="backups" marginBottom={3} />
                            <FlexGrow />
                            <BackupDecisionLogAboutView />
                        </div>

                        <div className="d-flex align-items-center gap-3 flex-wrap mb-4">
                            <ButtonWithSpinner
                                variant="primary"
                                icon="refresh"
                                isSpinning={asyncGetDecisionLog.loading}
                                onClick={() => asyncGetDecisionLog.execute()}
                            >
                                Refresh
                            </ButtonWithSpinner>
                            <Checkbox
                                selected={autoRefresh}
                                toggleSelection={(x) => setAutoRefresh(x.currentTarget.checked)}
                                type="switch"
                                color="primary"
                            >
                                Auto refresh (every {autoRefreshIntervalInMs / 1000}s)
                            </Checkbox>
                            <FlexGrow />
                            {result && (
                                <div className="small text-muted">
                                    Decisions are collected in memory on node{" "}
                                    <Badge bg="node" pill>
                                        {result.NodeTag}
                                    </Badge>
                                </div>
                            )}
                        </div>

                        {!result && asyncGetDecisionLog.loading && <LoadingView />}

                        {result && (
                            <>
                                <HrHeader>Queue</HrHeader>
                                <BackupDecisionLogSummary log={result} />

                                <HrHeader className="mt-4" count={result.TotalResults}>
                                    Decisions
                                </HrHeader>

                                <div className="d-flex gap-2 flex-wrap mb-3">
                                    <Form.Select
                                        className="w-auto"
                                        value={databaseFilter}
                                        onChange={(x) => setDatabaseFilter(x.currentTarget.value)}
                                        aria-label="Filter by database"
                                    >
                                        <option value={allDatabasesOption}>All databases</option>
                                        {result.Databases.map((databaseName) => (
                                            <option key={databaseName} value={databaseName}>
                                                {databaseName}
                                            </option>
                                        ))}
                                    </Form.Select>
                                    <Form.Select
                                        className="w-auto"
                                        value={kindFilter}
                                        onChange={(x) => setKindFilter(x.currentTarget.value)}
                                        aria-label="Filter by decision kind"
                                    >
                                        <option value={allKindsOption}>All kinds</option>
                                        {kindOptions.map((kind) => (
                                            <option key={kind} value={kind}>
                                                {kind}
                                            </option>
                                        ))}
                                    </Form.Select>
                                    <Form.Control
                                        type="text"
                                        className="w-auto flex-grow-1"
                                        style={{ maxWidth: "25rem" }}
                                        placeholder="Filter by reason..."
                                        value={searchText}
                                        onChange={(x) => setSearchText(x.currentTarget.value)}
                                        aria-label="Filter by reason"
                                    />
                                </div>

                                <BackupDecisionLogEntries
                                    entries={filteredEntries}
                                    totalResults={result.TotalResults}
                                />
                            </>
                        )}
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

import React, { memo, useCallback, useState } from "react";
import Table from "react-bootstrap/Table";
import Spinner from "react-bootstrap/Spinner";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { RichAlert } from "components/common/RichAlert";
import Badge from "react-bootstrap/Badge";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelActions,
    RichPanelDetails,
    RichPanelDetailItem,
} from "components/common/RichPanel";
import { Icon } from "components/common/Icon";
import Code, { CodeLanguage } from "components/common/Code";
import IndexUtils from "components/utils/IndexUtils";
import IconName from "typings/server/icons";

type IndexDefinition = Raven.Client.Documents.Indexes.IndexDefinition;
type IndexFieldOptions = Raven.Client.Documents.Indexes.IndexFieldOptions;

interface DatabaseIndexDefinitionsProps {
    packageId: string;
    database: string;
    node: string;
}

// On-demand per-node index definitions from the analyzer databases/indexes endpoint. The map/reduce
// functions render through the shared Code component (read-only counterpart of the live index editor).
export default memo(function DatabaseIndexDefinitions({ packageId, database, node }: DatabaseIndexDefinitionsProps) {
    const { manageServerService } = useServices();
    // collapsed by default - map/reduce bodies can be large; expand selectively or all at once
    const [expanded, setExpanded] = useState<Set<string>>(new Set());

    const definitions = useAsync(async () => {
        if (!node) {
            return [] as IndexDefinition[];
        }
        return manageServerService.getDebugPackageDatabaseIndexDefinitions(packageId, node, database);
    }, [packageId, node, database]);

    const indexes = definitions.result ?? [];
    const allExpanded = indexes.length > 0 && indexes.every((index) => expanded.has(index.Name));

    const toggleIndex = useCallback(
        (name: string) =>
            setExpanded((prev) => {
                const next = new Set(prev);
                if (next.has(name)) {
                    next.delete(name);
                } else {
                    next.add(name);
                }
                return next;
            }),
        []
    );

    const toggleAll = () => setExpanded(allExpanded ? new Set() : new Set(indexes.map((index) => index.Name)));

    return (
        <div className="database-index-definitions">
            <div className="panel-bg-1 rounded">
                <div className="p-4">
                    <div className="hstack gap-3 align-items-center mb-3 flex-wrap">
                        <h3 className="m-0">Index Definitions</h3>
                        {indexes.length > 0 && (
                            <Button variant="link" size="sm" className="ms-auto" onClick={toggleAll}>
                                <Icon icon={allExpanded ? "collapse" : "expand"} />
                                {allExpanded ? "Collapse all" : "Expand all"}
                            </Button>
                        )}
                    </div>

                    {definitions.loading ? (
                        <div className="panel-bg-1 rounded">
                            <div className="p-4">
                                <div className="hstack gap-2 justify-content-center text-muted py-3">
                                    <Spinner size="sm" /> Loading index definitions for node {node}...
                                </div>
                            </div>
                        </div>
                    ) : definitions.error ? (
                        <RichAlert variant="danger">
                            Could not load index definitions for node {node}. The package may not contain index
                            definitions for this database, or the report expired.
                        </RichAlert>
                    ) : indexes.length === 0 ? (
                        <div className="panel-bg-1 rounded">
                            <div className="p-4">
                                <EmptySet compact className="justify-content-center">
                                    No index definitions for {database} on node {node}
                                </EmptySet>
                            </div>
                        </div>
                    ) : (
                        <div className="vstack">
                            {indexes.map((index) => (
                                <IndexDefinitionPanel
                                    key={index.Name}
                                    index={index}
                                    expanded={expanded.has(index.Name)}
                                    onToggle={toggleIndex}
                                />
                            ))}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
});

interface IndexDefinitionPanelProps {
    index: IndexDefinition;
    expanded: boolean;
    onToggle: (name: string) => void;
}

const IndexDefinitionPanel = memo(function IndexDefinitionPanel({
    index,
    expanded,
    onToggle,
}: IndexDefinitionPanelProps) {
    const language: CodeLanguage = index.Type?.startsWith("JavaScript") ? "javascript" : "csharp";
    const maps = index.Maps ?? [];
    const fields = Object.entries(index.Fields ?? {});
    const sources = Object.keys(index.AdditionalSources ?? {});
    const configuration = Object.entries(index.Configuration ?? {});
    const extraDetails = collectExtraDetails(index);

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    <RichPanelName>{index.Name}</RichPanelName>
                </RichPanelInfo>
                <div className="hstack gap-3 flex-wrap align-items-center ms-auto">
                    <span className="hstack gap-1" title={indexTypeDescription(index.Type)}>
                        <Icon icon={IndexUtils.indexTypeIcon(index.Type) ?? "index"} margin="m-0" />
                        {IndexUtils.formatType(index.Type)}
                    </span>
                    <span className="hstack gap-1" title={sourceTypeDescription(index.SourceType)}>
                        <Icon icon={sourceTypeIcon(index.SourceType)} margin="m-0" />
                        {index.SourceType === "TimeSeries" ? "Time Series" : index.SourceType}
                    </span>
                    {index.State && index.State !== "Normal" && <Badge bg="warning">{index.State}</Badge>}
                    {index.OutputReduceToCollection && (
                        <Badge bg="success">
                            <Icon icon="documents" margin="m-0" /> Output to {index.OutputReduceToCollection}
                        </Badge>
                    )}
                    <span className="hstack gap-1" title="Index scheduling priority">
                        <Icon icon={priorityIcon(index.Priority)} margin="m-0" />
                        {index.Priority ?? "Normal"}
                    </span>
                    <span className="hstack gap-1" title="Index lock mode">
                        <Icon icon={lockIcon(index.LockMode)} margin="m-0" />
                        {lockLabel(index.LockMode)}
                    </span>
                </div>
                <RichPanelActions>
                    <Button
                        variant="secondary"
                        active={expanded}
                        onClick={() => onToggle(index.Name)}
                        title={expanded ? "Collapse" : "Expand"}
                        className="btn-toggle-panel"
                    >
                        <Icon icon={expanded ? "fold" : "unfold"} margin="m-0" />
                    </Button>
                </RichPanelActions>
            </RichPanelHeader>
            <Collapse in={expanded} unmountOnExit>
                <div className="index-definition-body">
                    {extraDetails.length > 0 && (
                        <RichPanelDetails>
                            {extraDetails.map((detail) => (
                                <RichPanelDetailItem key={detail.label} label={detail.label}>
                                    {detail.value}
                                </RichPanelDetailItem>
                            ))}
                        </RichPanelDetails>
                    )}
                    <div className="p-4 vstack gap-3">
                        {maps.map((map, i) => (
                            <div key={i}>
                                <div className="small-label mb-1">{maps.length > 1 ? `Map ${i + 1}` : "Map"}</div>
                                <Code language={language} code={map} />
                            </div>
                        ))}
                        {index.Reduce && (
                            <div>
                                <div className="small-label mb-1">Reduce</div>
                                <Code language={language} code={index.Reduce} />
                            </div>
                        )}
                        {fields.length > 0 && <FieldsTable fields={index.Fields} />}
                        {sources.length > 0 && (
                            <div>
                                <div className="small-label mb-1">Additional sources</div>
                                <div className="hstack gap-1 flex-wrap">
                                    {sources.map((name) => (
                                        <Badge key={name} bg="secondary">
                                            {name}
                                        </Badge>
                                    ))}
                                </div>
                            </div>
                        )}
                        {configuration.length > 0 && (
                            <div>
                                <div className="small-label mb-1">Configuration</div>
                                <div className="vstack">
                                    {configuration.map(([key, value]) => (
                                        <div key={key} className="small">
                                            <span className="fw-bold">{key}</span>: {String(value)}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}
                    </div>
                </div>
            </Collapse>
        </RichPanel>
    );
});

function indexTypeDescription(type: IndexDefinition["Type"]): string {
    switch (type) {
        case "Map":
            return "Index type: Map";
        case "MapReduce":
            return "Index type: Map-Reduce";
        case "AutoMap":
            return "Index type: Auto Map (automatically created by RavenDB)";
        case "AutoMapReduce":
            return "Index type: Auto Map-Reduce (automatically created by RavenDB)";
        case "JavaScriptMap":
            return "Index type: Map (JavaScript)";
        case "JavaScriptMapReduce":
            return "Index type: Map-Reduce (JavaScript)";
        case "Faulty":
            return "Index type: Faulty — the index is in an error state";
        default:
            return null;
    }
}

function priorityIcon(priority: IndexDefinition["Priority"]): IconName {
    switch (priority) {
        case "Low":
            return "coffee";
        case "High":
            return "force";
        default:
            return "check";
    }
}

function lockIcon(lockMode: IndexDefinition["LockMode"]): IconName {
    switch (lockMode) {
        case "LockedIgnore":
            return "lock";
        case "LockedError":
            return "lock-error";
        default:
            return "unlock";
    }
}

function lockLabel(lockMode: IndexDefinition["LockMode"]): string {
    switch (lockMode) {
        case "LockedIgnore":
            return "Locked (Ignore)";
        case "LockedError":
            return "Locked (Error)";
        default:
            return "Unlocked";
    }
}

function sourceTypeIcon(sourceType: IndexDefinition["SourceType"]): IconName {
    switch (sourceType) {
        case "TimeSeries":
            return "timeseries";
        case "Counters":
            return "new-counter";
        default:
            return "documents";
    }
}

function sourceTypeDescription(sourceType: IndexDefinition["SourceType"]): string {
    switch (sourceType) {
        case "Documents":
            return "Source type: Documents";
        case "TimeSeries":
            return "Source type: Time Series";
        case "Counters":
            return "Source type: Counters";
        default:
            return null;
    }
}

function FieldsTable({ fields }: { fields: { [key: string]: IndexFieldOptions } }) {
    const entries = Object.entries(fields);

    return (
        <div>
            <div className="small-label mb-1">Fields ({entries.length})</div>
            <Table responsive className="m-0 align-middle">
                <thead>
                    <tr>
                        <th>Field</th>
                        <th>Indexing</th>
                        <th>Storage</th>
                        <th>Analyzer</th>
                        <th>Suggestions</th>
                        <th>Term vector</th>
                    </tr>
                </thead>
                <tbody>
                    {entries.map(([name, options]) => (
                        <tr key={name}>
                            <td className="fw-bold">{name}</td>
                            <td>{options.Indexing ?? "-"}</td>
                            <td>{options.Storage ?? "-"}</td>
                            <td>{options.Analyzer ?? "-"}</td>
                            <td>{options.Suggestions ? "Yes" : "-"}</td>
                            <td>{options.TermVector ?? "-"}</td>
                        </tr>
                    ))}
                </tbody>
            </Table>
        </div>
    );
}

// Map-reduce-output, deployment and assembly properties that the live editor surfaces but that the
// summary/stats views omit - shown as detail chips only when the index actually sets them.
function collectExtraDetails(index: IndexDefinition): { label: string; value: string }[] {
    const details: { label: string; value: string }[] = [];

    if (index.PatternReferencesCollectionName) {
        details.push({ label: "References collection", value: index.PatternReferencesCollectionName });
    }
    if (index.PatternForOutputReduceToCollectionReferences) {
        details.push({ label: "Output reference pattern", value: index.PatternForOutputReduceToCollectionReferences });
    }
    if (index.ReduceOutputIndex != null) {
        details.push({ label: "Reduce output index", value: String(index.ReduceOutputIndex) });
    }
    if (index.DeploymentMode) {
        details.push({ label: "Deployment mode", value: index.DeploymentMode });
    }
    if (index.ArchivedDataProcessingBehavior) {
        details.push({ label: "Archived data", value: index.ArchivedDataProcessingBehavior });
    }
    if (index.CompoundFields?.length) {
        details.push({
            label: "Compound fields",
            value: index.CompoundFields.map((group) => group.join(" + ")).join(", "),
        });
    }
    if (index.AdditionalAssemblies?.length) {
        details.push({ label: "Additional assemblies", value: String(index.AdditionalAssemblies.length) });
    }

    return details;
}

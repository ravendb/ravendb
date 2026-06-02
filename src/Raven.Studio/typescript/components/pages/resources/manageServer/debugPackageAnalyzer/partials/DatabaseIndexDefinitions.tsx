import React, { useState } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import Spinner from "react-bootstrap/Spinner";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { RichAlert } from "components/common/RichAlert";
import { StatePill } from "components/common/StatePill";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelDetails,
    RichPanelDetailItem,
} from "components/common/RichPanel";
import { Icon } from "components/common/Icon";
import Code, { CodeLanguage } from "components/common/Code";
import Select, { SelectOption } from "components/common/select/Select";

type IndexDefinition = Raven.Client.Documents.Indexes.IndexDefinition;
type IndexFieldOptions = Raven.Client.Documents.Indexes.IndexFieldOptions;

interface DatabaseIndexDefinitionsProps {
    packageId: string;
    database: string;
    nodes: string[];
}

// On-demand per-node index definitions from the analyzer databases/indexes endpoint. The map/reduce
// functions render through the shared Code component (read-only counterpart of the live index editor).
export default function DatabaseIndexDefinitions({ packageId, database, nodes }: DatabaseIndexDefinitionsProps) {
    const { manageServerService } = useServices();
    const [selectedNode, setSelectedNode] = useState<string>(nodes[0] ?? null);
    // collapsed by default - map/reduce bodies can be large; expand selectively or all at once
    const [expanded, setExpanded] = useState<Set<string>>(new Set());

    const definitions = useAsync(async () => {
        if (!selectedNode) {
            return [] as IndexDefinition[];
        }
        return manageServerService.getDebugPackageDatabaseIndexDefinitions(packageId, selectedNode, database);
    }, [packageId, selectedNode, database]);

    const nodeOptions: SelectOption<string>[] = nodes.map((tag) => ({ value: tag, label: `Node ${tag}` }));
    const indexes = definitions.result ?? [];
    const allExpanded = indexes.length > 0 && indexes.every((index) => expanded.has(index.Name));

    const toggleIndex = (name: string) =>
        setExpanded((prev) => {
            const next = new Set(prev);
            if (next.has(name)) {
                next.delete(name);
            } else {
                next.add(name);
            }
            return next;
        });

    const toggleAll = () => setExpanded(allExpanded ? new Set() : new Set(indexes.map((index) => index.Name)));

    return (
        <div className="database-index-definitions">
            <div className="hstack gap-3 align-items-center mb-3 flex-wrap">
                <h3 className="m-0">Index Definitions</h3>
                {nodes.length > 1 && (
                    <div className="node-select">
                        <Select
                            options={nodeOptions}
                            value={nodeOptions.find((o) => o.value === selectedNode)}
                            onChange={(option) => option && setSelectedNode(option.value)}
                            isSearchable={false}
                            isRoundedPill
                        />
                    </div>
                )}
                {indexes.length > 0 && (
                    <Button variant="link" size="sm" className="ms-auto" onClick={toggleAll}>
                        <Icon icon={allExpanded ? "collapse" : "expand"} />
                        {allExpanded ? "Collapse all" : "Expand all"}
                    </Button>
                )}
            </div>

            {definitions.loading ? (
                <Card>
                    <Card.Body>
                        <div className="hstack gap-2 justify-content-center text-muted py-3">
                            <Spinner size="sm" /> Loading index definitions for node {selectedNode}...
                        </div>
                    </Card.Body>
                </Card>
            ) : definitions.error ? (
                <RichAlert variant="danger">
                    Could not load index definitions for node {selectedNode}. The package may not contain index
                    definitions for this database, or the report expired.
                </RichAlert>
            ) : indexes.length === 0 ? (
                <Card>
                    <Card.Body>
                        <EmptySet compact>
                            No index definitions for {database} on node {selectedNode}
                        </EmptySet>
                    </Card.Body>
                </Card>
            ) : (
                <div className="vstack gap-2">
                    {indexes.map((index) => (
                        <IndexDefinitionPanel
                            key={index.Name}
                            index={index}
                            expanded={expanded.has(index.Name)}
                            onToggle={() => toggleIndex(index.Name)}
                        />
                    ))}
                </div>
            )}
        </div>
    );
}

interface IndexDefinitionPanelProps {
    index: IndexDefinition;
    expanded: boolean;
    onToggle: () => void;
}

function IndexDefinitionPanel({ index, expanded, onToggle }: IndexDefinitionPanelProps) {
    const language: CodeLanguage = index.Type?.startsWith("JavaScript") ? "javascript" : "csharp";
    const maps = index.Maps ?? [];
    const fields = Object.entries(index.Fields ?? {});
    const sources = Object.keys(index.AdditionalSources ?? {});
    const configuration = Object.entries(index.Configuration ?? {});
    const extraDetails = collectExtraDetails(index);

    return (
        <RichPanel>
            <RichPanelHeader onClick={onToggle} style={{ cursor: "pointer" }}>
                <RichPanelInfo>
                    <RichPanelName>
                        <Icon icon={expanded ? "chevron-down" : "chevron-right"} margin="m-0 me-1" />
                        {index.Name}
                    </RichPanelName>
                </RichPanelInfo>
                <div className="hstack gap-1 flex-wrap align-items-center">
                    <StatePill bg="info">{index.Type}</StatePill>
                    <StatePill bg="secondary">{index.SourceType}</StatePill>
                    {index.State && index.State !== "Normal" && <StatePill bg="warning">{index.State}</StatePill>}
                    {index.OutputReduceToCollection && (
                        <StatePill bg="success">
                            <Icon icon="documents" margin="m-0" /> Output to {index.OutputReduceToCollection}
                        </StatePill>
                    )}
                    <span className="small-label">
                        Priority: {index.Priority ?? "Normal"} · Lock: {index.LockMode ?? "Unlock"}
                    </span>
                </div>
            </RichPanelHeader>
            <Collapse in={expanded}>
                <div>
                    {extraDetails.length > 0 && (
                        <RichPanelDetails>
                            {extraDetails.map((detail) => (
                                <RichPanelDetailItem key={detail.label} label={detail.label}>
                                    {detail.value}
                                </RichPanelDetailItem>
                            ))}
                        </RichPanelDetails>
                    )}
                    <div className="p-3 vstack gap-3">
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
                                        <StatePill key={name} bg="secondary">
                                            {name}
                                        </StatePill>
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

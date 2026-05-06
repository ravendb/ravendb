import React, { useState } from "react";
import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import { Icon } from "components/common/Icon";
import { StickyHeader } from "components/common/StickyHeader";
import copyToClipboard from "common/copyToClipboard";
import { MethodEntry, MethodGroup } from "./sampleQueriesTypes";

interface MethodsTableProps {
    methodGroups: MethodGroup[];
}

export default function MethodsTable({ methodGroups }: MethodsTableProps) {
    const [search, setSearch] = useState("");

    const filteredGroups = methodGroups
        .map((group) => ({
            ...group,
            methods: group.methods.filter((method) => method.signature.toLowerCase().includes(search.toLowerCase())),
        }))
        .filter((group) => group.methods.length > 0);

    return (
        <div className="methods-table vstack gap-3 px-3 py-1">
            <StickyHeader className="panel-bg-1">
                <Form.Control
                    type="search"
                    placeholder="Search by signature"
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                />
            </StickyHeader>
            {filteredGroups.map((group) => (
                <MethodGroupCard key={group.category} group={group} />
            ))}
        </div>
    );
}

interface MethodGroupCardProps {
    group: MethodGroup;
}

function MethodGroupCard({ group }: MethodGroupCardProps) {
    return (
        <div>
            <h6 className="mb-2">{group.category}</h6>
            <Card className="border border-color-light rounded overflow-hidden mb-0">
                <Row className="mx-0 panel-bg-2 fw-semibold border-bottom border-color-light">
                    <Col xs={6} className="py-2 px-3">
                        Methods signature
                    </Col>
                    <Col xs={6} className="py-2 px-3 border-start border-color-light">
                        Description
                    </Col>
                </Row>
                {group.methods.map((method) => (
                    <MethodRow key={method.signature} method={method} />
                ))}
            </Card>
        </div>
    );
}

interface MethodRowProps {
    method: MethodEntry;
}

function MethodRow({ method }: MethodRowProps) {
    const handleCopy = () => {
        copyToClipboard.copy(method.signature, "Method signature copied to clipboard");
    };

    return (
        <Row className="mx-0 border-top border-color-light method-row">
            <Col xs={6} className="py-2 px-3 position-relative">
                <code>{method.signature}</code>
                <button
                    type="button"
                    className="copy-btn position-absolute top-0 end-0 px-2 py-1 border-0 bg-transparent rounded-1 cursor-pointer"
                    aria-label="Copy method signature to clipboard"
                    onClick={handleCopy}
                >
                    <Icon icon="copy-to-clipboard" margin="m-0" />
                </button>
            </Col>
            <Col xs={6} className="py-2 px-3 border-start border-color-light">
                {method.description}
            </Col>
        </Row>
    );
}

import React, { useState } from "react";
import Form from "react-bootstrap/Form";
import { MethodEntry, MethodGroup } from "./sampleQueriesTypes";
import copyToClipboard from "common/copyToClipboard";
import { Icon } from "components/common/Icon";
import useBoolean from "components/hooks/useBoolean";
import { StickyHeader } from "components/common/StickyHeader";
import useDebouncedInput from "components/hooks/useDebouncedInput";

interface MethodsTableProps {
    methodGroups: MethodGroup[];
}

export default function MethodsTable({ methodGroups }: MethodsTableProps) {
    const [debouncedSearch, setDebouncedSearch] = useState("");
    const { localValue: search, handleChange } = useDebouncedInput<string>({
        value: "",
        onDebouncedUpdate: (value) => setDebouncedSearch(value),
    });

    const filteredGroups = methodGroups
        .map((group) => ({
            ...group,
            methods: group.methods.filter((method) => {
                const term = debouncedSearch.toLowerCase();
                return method.signature.toLowerCase().includes(term) || method.description.toLowerCase().includes(term);
            }),
        }))
        .filter((group) => group.methods.length > 0);

    return (
        <div className="vstack gap-3 px-3 py-1">
            <StickyHeader className="panel-bg-1">
                <Form.Control
                    placeholder="Search methods"
                    value={search}
                    onChange={(e) => handleChange(e.target.value)}
                />
            </StickyHeader>
            {filteredGroups.map((group) => (
                <div key={group.category}>
                    <h6 className="mb-2">{group.category}</h6>
                    <table className="rounded table table-sm table-bordered mb-0" style={{ tableLayout: "fixed" }}>
                        <thead className="panel-bg-2 border-1 border-color-light">
                            <tr>
                                <th style={{ width: "50%" }}>Methods signature</th>
                                <th style={{ width: "50%" }}>Description</th>
                            </tr>
                        </thead>
                        <tbody>
                            {group.methods.map((method) => (
                                <MethodRow key={method.signature} method={method} />
                            ))}
                        </tbody>
                    </table>
                </div>
            ))}
        </div>
    );
}

interface MethodRowProps {
    method: MethodEntry;
}

function MethodRow({ method }: MethodRowProps) {
    const { value: isHovered, setTrue, setFalse } = useBoolean(false);

    const handleCopy = () => {
        copyToClipboard.copy(method.signature, "Method signature copied to clipboard");
    };

    return (
        <tr>
            <td onMouseEnter={setTrue} onMouseLeave={setFalse} className="position-relative">
                <code className="text-info" style={{ backgroundColor: "rgba(var(--bs-info-rgb), 0.1)" }}>
                    {method.signature}
                </code>
                <span
                    className="position-absolute top-0 end-0 px-2 py-1 rounded-1 cursor-pointer"
                    style={{ opacity: isHovered ? 1 : 0, transition: "opacity 0.15s ease" }}
                >
                    <Icon icon="copy-to-clipboard" margin="m-0" onClick={handleCopy} />
                </span>
            </td>
            <td>
                {method.description} (
                <code className="text-info" style={{ backgroundColor: "rgba(var(--bs-info-rgb), 0.1)" }}>
                    {method.returnType}
                </code>
                )
            </td>
        </tr>
    );
}

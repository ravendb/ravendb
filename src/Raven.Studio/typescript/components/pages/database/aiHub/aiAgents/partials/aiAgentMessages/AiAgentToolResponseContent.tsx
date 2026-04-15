import { useReactTable, getCoreRowModel, getSortedRowModel, getFilteredRowModel } from "@tanstack/react-table";
import AceEditor from "components/common/ace/AceEditor";
import { aceEditorUtils } from "components/common/ace/aceEditorUtils";
import { useDocumentColumnsProvider } from "components/common/virtualTable/columnProviders/useDocumentColumnsProvider";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { useRef, useMemo } from "react";
import ReactAce from "react-ace";
import document from "models/database/documents/document";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";

interface AiAgentToolResponseContentProps {
    content: string;
}

export function AiAgentToolResponseContent({ content }: AiAgentToolResponseContentProps) {
    const isTable = content.startsWith("[") && content.endsWith("]") && content.length > 2;

    if (isTable) {
        return <TableContent content={content} />;
    }

    return <AceEditorContent content={content} />;
}

function TableContent({ content }: AiAgentToolResponseContentProps) {
    const tableData = useMemo(() => JSON.parse(content).map((x: any) => new document(x)), [content]);

    const { columnDefs } = useDocumentColumnsProvider({
        documents: tableData,
        availableWidth: window.innerWidth,
        hasCheckbox: false,
        hasPreview: false,
        hasFlags: true,
    });

    const table = useReactTable({
        data: tableData,
        columns: columnDefs,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    const heightInPx = virtualTableUtils.getHeightInPx(tableData.length, 300);

    return <VirtualTable table={table} heightInPx={heightInPx} className="border border-secondary" />;
}

function AceEditorContent({ content }: AiAgentToolResponseContentProps) {
    const aceRef = useRef<ReactAce>(null);
    const contentMode = aceEditorUtils.getAceEditorMode(content);

    return (
        <AceEditor
            aceRef={aceRef}
            defaultValue={content}
            readOnly
            mode={contentMode}
            height={aceEditorUtils.getAceEditorHeight(content, { maxLineCount: 6 })}
            actions={[
                { component: <AceEditor.FullScreenAction /> },
                { component: <AceEditor.FormatAction /> },
                { component: <AceEditor.AutoResizeHeightAction /> },
            ]}
        />
    );
}

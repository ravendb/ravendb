import appUrl from "common/appUrl";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { Icon } from "components/common/Icon";
import { MultiRadioToggle } from "components/common/toggles/MultiRadioToggle";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { InputItem } from "components/models/common";
import { useAppSelector } from "components/store";
import endpoints from "endpoints";
import { useRef, useState } from "react";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";

interface ExportDocumentsDropdownProps {
    collectionName: string;
    // fields of the columns shown in the table, exported instead of all the fields when chosen
    getVisibleColumnFields: () => string[];
}

type ExportFormat = "csv" | "json";
type ExportColumns = "all" | "visible";

const formatItems: InputItem<ExportFormat>[] = [
    { label: "CSV", value: "csv" },
    { label: "JSON", value: "json" },
];

const columnsItems: InputItem<ExportColumns>[] = [
    { label: "All", value: "all" },
    { label: "Visible", value: "visible" },
];

export default function ExportDocumentsDropdown({
    collectionName,
    getVisibleColumnFields,
}: ExportDocumentsDropdownProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { reportEvent } = useEventsCollector();

    const [format, setFormat] = useState<ExportFormat>("csv");
    const [columns, setColumns] = useState<ExportColumns>("all");
    const [isOpen, setIsOpen] = useState(false);

    const formRef = useRef<HTMLFormElement>(null);
    const exportOptionsRef = useRef<HTMLInputElement>(null);

    const handleExport = () => {
        reportEvent("query", "export-csv");

        const args = {
            format,
            field: columns === "all" ? undefined : getVisibleColumnFields(),
        };

        exportOptionsRef.current.value = JSON.stringify({ Query: `from '${collectionName}'` });
        formRef.current.action =
            appUrl.forDatabaseQuery(databaseName) +
            endpoints.databases.streaming.streamsQueries +
            appUrl.urlEncodeArgs(args);
        formRef.current.submit();

        setIsOpen(false);
    };

    return (
        <>
            <Dropdown show={isOpen} onToggle={setIsOpen} autoClose="outside">
                <Dropdown.Toggle as={CustomDropdownToggle} variant="secondary" title="Export documents as CSV/JSON">
                    <Icon icon="csv-export" />
                    Export to file
                </Dropdown.Toggle>
                <Dropdown.Menu align="end" className="p-3" style={{ minWidth: "170px" }}>
                    <div className="vstack gap-3">
                        <MultiRadioToggle<ExportFormat>
                            label="Format"
                            inputItems={formatItems}
                            selectedItem={format}
                            setSelectedItem={setFormat}
                        />
                        <MultiRadioToggle<ExportColumns>
                            label="Columns"
                            inputItems={columnsItems}
                            selectedItem={columns}
                            setSelectedItem={setColumns}
                        />
                        <Button variant="primary" onClick={handleExport} className="align-self-start">
                            <Icon icon="download" />
                            Export
                        </Button>
                    </div>
                </Dropdown.Menu>
            </Dropdown>
            <form ref={formRef} method="post" target="hidden-form" className="d-none" data-testid="export-form">
                <input ref={exportOptionsRef} type="hidden" name="ExportOptions" />
            </form>
        </>
    );
}

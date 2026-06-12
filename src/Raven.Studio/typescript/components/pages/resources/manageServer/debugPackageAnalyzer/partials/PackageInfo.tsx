import React from "react";
import Button from "react-bootstrap/Button";

interface PackageInfoProps {
    fileName: string;
    onReset: () => void;
}

export default function PackageInfo({ fileName, onReset }: PackageInfoProps) {
    const created = parseCreatedDate(fileName);

    return (
        <div className="package-info">
            <div className="d-flex align-items-center justify-content-between gap-3 mb-1">
                <small>Selected package{created ? ` (created: ${created})` : ""}</small>
                <Button variant="link" size="sm" className="p-0 flex-shrink-0" onClick={onReset}>
                    Reset
                </Button>
            </div>
            <input type="text" readOnly className="form-control" value={fileName ?? ""} onChange={() => undefined} />
        </div>
    );
}

// debug package file names are produced as e.g. "2025-11-19 10-55-11 Cluster Wide.zip"
function parseCreatedDate(fileName: string): string {
    if (!fileName) {
        return null;
    }

    const match = fileName.match(/(\d{4})-(\d{2})-(\d{2})[ _](\d{2})-(\d{2})-(\d{2})/);
    if (!match) {
        return null;
    }

    const [, year, month, day, hour, minute, second] = match;
    const date = new Date(Number(year), Number(month) - 1, Number(day), Number(hour), Number(minute), Number(second));
    if (isNaN(date.getTime())) {
        return null;
    }

    return date.toLocaleString();
}

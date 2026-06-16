import React from "react";
import moment from "moment";
import Button from "react-bootstrap/Button";
import genUtils from "common/generalUtils";

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

// debug package file names are produced as e.g. "2025-11-19 10-55-11 Cluster Wide.zip" - the date and time
// are the first two space-separated tokens
function parseCreatedDate(fileName: string): string {
    if (!fileName) {
        return null;
    }

    const [datePart, timePart] = fileName.split(" ");
    const created = moment(`${datePart} ${timePart ?? ""}`.trim(), "YYYY-MM-DD HH-mm-ss", true);
    return created.isValid() ? created.format(genUtils.dateFormat) : null;
}

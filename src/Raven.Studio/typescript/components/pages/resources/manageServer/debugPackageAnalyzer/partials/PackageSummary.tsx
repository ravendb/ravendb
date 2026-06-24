import React from "react";
import moment from "moment";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import genUtils from "common/generalUtils";
import AnalysisVerdict from "./AnalysisVerdict";
import { FlatIssue } from "./analyzerUtils";
import "./PackageSummary.scss";

interface PackageSummaryProps {
    fileName: string;
    issues: FlatIssue[];
    onReset: () => void;
    onViewIssues?: () => void;
}

// The analyzed package's identity (file name + created date + reset) merged with its whole-package
// health verdict, shown as a single card next to the page title.
export default function PackageSummary({ fileName, issues, onReset, onViewIssues }: PackageSummaryProps) {
    const created = parseCreatedDate(fileName);

    return (
        <div className="package-summary">
            <div className="package-summary-identity">
                <Icon icon="gather-debug-information" className="package-summary-icon" margin="m-0" />
                <div className="package-summary-file">
                    <div className="package-summary-name" title={fileName ?? ""}>
                        {fileName}
                    </div>
                    {created && <div className="package-summary-created lh-1 text-muted">Created {created}</div>}
                </div>
                <Button variant="secondary" size="sm" className="rounded-pill flex-shrink-0" onClick={onReset}>
                    <Icon icon="refresh" /> Reset
                </Button>
            </div>
            <div className="package-summary-verdict">
                <AnalysisVerdict issues={issues} onViewIssues={onViewIssues} />
            </div>
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

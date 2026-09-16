import React, { useMemo } from "react";
import classNames from "classnames";
import { AboutViewHeading } from "components/common/AboutView";
import { useDebugPackageAnalyzer } from "./useDebugPackageAnalyzer";
import DebugPackageUpload from "./partials/DebugPackageUpload";
import AnalysisError from "./partials/AnalysisError";
import DebugPackageAnalysisView from "./partials/DebugPackageAnalysisView";
import PackageSummary from "./partials/PackageSummary";
import { flattenIssues } from "./partials/analyzerUtils";
import "./DebugPackage.scss";
import appUrl = require("common/appUrl");

export default function DebugPackage() {
    const { view, onFileSelected, summary, fileName, reset, isAnalyzing, error } = useDebugPackageAnalyzer();

    const isUploadView = view === "upload" || view === "analyzing";

    const issues = useMemo(() => (summary ? flattenIssues(summary) : []), [summary]);

    const scrollToResults = () => {
        document.getElementById("debug-package-analysis-results")?.scrollIntoView({ block: "start" });
    };

    return (
        <div className={classNames("flex-window padding-xs", { "debug-package-window-fill": isUploadView })}>
            <div
                id="debug-package-analyzer"
                className={classNames("bs5 debug-package-analyzer content-margin", {
                    "debug-package-analyzer--fill": isUploadView,
                })}
            >
                <div className="d-flex justify-content-between align-items-start gap-3 flex-wrap mb-4">
                    <div>
                        <AboutViewHeading
                            title="Debug Package Analyzer"
                            icon="gather-debug-information"
                            iconAddon="search"
                            marginBottom={1}
                            backUrl={appUrl.forDebugPackage()}
                        />
                        <p className="text-muted fs-5 mb-0">
                            Examine the package to identify the problem with your server
                        </p>
                    </div>
                    {view === "loaded" && (
                        <PackageSummary
                            fileName={fileName}
                            issues={issues}
                            onReset={reset}
                            onViewIssues={scrollToResults}
                        />
                    )}
                </div>
                {isUploadView && (
                    <DebugPackageUpload isAnalyzing={isAnalyzing} fileName={fileName} onFileSelected={onFileSelected} />
                )}
                {view === "error" && <AnalysisError error={error} onReset={reset} />}
                {view === "loaded" && <DebugPackageAnalysisView summary={summary} />}
            </div>
        </div>
    );
}

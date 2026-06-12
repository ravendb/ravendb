import React from "react";
import { AboutViewHeading } from "components/common/AboutView";
import { useDebugPackageAnalyzer } from "./useDebugPackageAnalyzer";
import DebugPackageUpload from "./partials/DebugPackageUpload";
import AnalysisLoading from "./partials/AnalysisLoading";
import AnalysisError from "./partials/AnalysisError";
import DebugPackageAnalysisView from "./partials/DebugPackageAnalysisView";
import "./DebugPackage.scss";
import appUrl = require("common/appUrl");

export default function DebugPackage() {
    const { view, selectedFile, setSelectedFile, summary, fileName, analyze, reset, isAnalyzing, error } =
        useDebugPackageAnalyzer();

    return (
        <div className="flex-window padding-xs">
            <div id="debug-package-analyzer" className="bs5 debug-package-analyzer content-margin">
                <AboutViewHeading
                    title="Debug Package Analyzer"
                    icon="gather-debug-information"
                    iconAddon="search"
                    marginBottom={1}
                    backUrl={appUrl.forDebugPackage()}
                />
                <p className="text-muted fs-5 mb-4">Examine the package to identify the problem with your server</p>
                {view === "upload" && (
                    <DebugPackageUpload
                        hasFile={!!selectedFile}
                        isAnalyzing={isAnalyzing}
                        onFileSelected={setSelectedFile}
                        onAnalyze={analyze}
                    />
                )}
                {view === "analyzing" && <AnalysisLoading />}
                {view === "error" && <AnalysisError error={error} onReset={reset} />}
                {view === "loaded" && (
                    <DebugPackageAnalysisView summary={summary} fileName={fileName} onReset={reset} />
                )}
            </div>
        </div>
    );
}

import React from "react";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import GatherDebugInfo from "components/pages/resources/manageServer/gatherDebugInfo/GatherDebugInfo";
import { useDebugPackageAnalyzer } from "./useDebugPackageAnalyzer";
import DebugPackageUpload from "./partials/DebugPackageUpload";
import AnalysisLoading from "./partials/AnalysisLoading";
import AnalysisError from "./partials/AnalysisError";
import DebugPackageAnalysisView from "./partials/DebugPackageAnalysisView";
import "./DebugPackageAnalyzer.scss";

// Unified "Debug Package" page: create a package (left) or analyze an existing one (right). Once a
// package is analyzed the results take over the full width and the create/upload landing is replaced.
export default function DebugPackage() {
    const { view, selectedFile, setSelectedFile, summary, fileName, analyze, reset, isAnalyzing, error } =
        useDebugPackageAnalyzer();

    if (view === "loaded") {
        return (
            <div className="flex-window padding-xs">
                <div id="debug-package-analyzer" className="bs5 debug-package-analyzer content-margin">
                    <AboutViewHeading
                        title="Debug Package Analyzer"
                        icon="gather-debug-information"
                        iconAddon="search"
                        marginBottom={1}
                    />
                    <p className="text-muted fs-5 mb-4">Examine the package to identify the problem with your server</p>
                    <DebugPackageAnalysisView summary={summary} fileName={fileName} onReset={reset} />
                </div>
            </div>
        );
    }

    return (
        <div className="flex-window padding-xs">
            <div className="bs5 debug-package content-margin">
                <Row className="gy-4">
                    <Col xs={12} lg={6}>
                        <GatherDebugInfo />
                    </Col>
                    <Col xs={12} lg={6} className="debug-package-analyzer">
                        <Card>
                            <Card.Body className="d-flex flex-center flex-column">
                                <Icon
                                    icon="gather-debug-information"
                                    addon="search"
                                    margin="m-0"
                                    className="debug-package-analyze-icon text-info"
                                />
                                <h3 className="mt-3">Analyze Debug Package</h3>
                                <p className="lead text-center w-75 fs-5">
                                    Upload an existing debug package to identify the problem with your server.
                                </p>
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
                            </Card.Body>
                        </Card>
                    </Col>
                </Row>
            </div>
        </div>
    );
}

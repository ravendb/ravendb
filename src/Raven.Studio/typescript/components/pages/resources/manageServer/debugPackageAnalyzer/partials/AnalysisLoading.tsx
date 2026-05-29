import React from "react";
import Spinner from "react-bootstrap/Spinner";

export default function AnalysisLoading() {
    return (
        <div className="debug-package-analyzing vstack align-items-center justify-content-center py-5 gap-3">
            <Spinner />
            <span className="text-muted">Analyzing the debug package...</span>
        </div>
    );
}

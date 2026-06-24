import React from "react";
import { Icon } from "components/common/Icon";
import "./AnalysisProgress.scss";

interface AnalysisProgressProps {
    fileName?: string;
}

export default function AnalysisProgress({ fileName }: AnalysisProgressProps) {
    return (
        <div className="analysis-progress">
            <Icon icon="search" className="fs-2" margin="m-0" />
            <div className="analysis-progress-text">
                <span className="analysis-progress-title">Analysis in progress</span>
                {fileName && <span className="text-muted">{fileName}</span>}
            </div>
            <div
                className="analysis-progress-bar"
                role="progressbar"
                aria-label="Analysis in progress"
                aria-busy="true"
            >
                <div className="analysis-progress-bar-fill" />
            </div>
        </div>
    );
}

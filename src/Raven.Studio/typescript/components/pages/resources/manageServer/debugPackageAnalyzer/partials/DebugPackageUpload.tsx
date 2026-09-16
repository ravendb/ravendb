import React from "react";
import FileDropzone from "components/common/FileDropzone";
import AnalysisProgress from "./AnalysisProgress";

interface DebugPackageUploadProps {
    isAnalyzing: boolean;
    fileName?: string;
    onFileSelected: (file: File) => void;
}

export default function DebugPackageUpload(props: DebugPackageUploadProps) {
    const { isAnalyzing, fileName, onFileSelected } = props;

    if (isAnalyzing) {
        return (
            <div className="debug-package-upload vstack gap-3">
                <AnalysisProgress fileName={fileName} />
            </div>
        );
    }

    return (
        <div className="debug-package-upload vstack gap-3">
            <FileDropzone
                className="debug-package-dropzone"
                onChange={(files) => onFileSelected(files[0] ?? null)}
                validExtensions={["zip"]}
                maxFiles={1}
            />
        </div>
    );
}

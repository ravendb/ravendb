import React from "react";
import FileDropzone from "components/common/FileDropzone";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

interface DebugPackageUploadProps {
    hasFile: boolean;
    isAnalyzing: boolean;
    onFileSelected: (file: File) => void;
    onAnalyze: () => void;
}

export default function DebugPackageUpload(props: DebugPackageUploadProps) {
    const { hasFile, isAnalyzing, onFileSelected, onAnalyze } = props;

    return (
        <div className="debug-package-upload vstack gap-3">
            <div>
                <ButtonWithSpinner
                    variant="primary"
                    className="rounded-pill"
                    icon="search"
                    isSpinning={isAnalyzing}
                    disabled={!hasFile}
                    onClick={onAnalyze}
                >
                    Analyze package
                </ButtonWithSpinner>
            </div>
            <FileDropzone
                onChange={(files) => onFileSelected(files[0] ?? null)}
                validExtensions={["zip"]}
                maxFiles={1}
            />
        </div>
    );
}

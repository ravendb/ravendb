import { useCallback, useEffect, useState } from "react";
import { useServices } from "hooks/useServices";
import { useAsyncCallback } from "react-async-hook";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

export type DebugPackageAnalyzerView = "upload" | "analyzing" | "error" | "loaded";

export function useDebugPackageAnalyzer() {
    const { manageServerService } = useServices();

    const [selectedFile, setSelectedFile] = useState<File>(null);
    const [summary, setSummary] = useState<DebugPackageAnalysisSummary>(null);
    const [fileName, setFileName] = useState<string>(null);
    const [error, setError] = useState<any>(null);

    const analyzeAsync = useAsyncCallback((file: File) => manageServerService.uploadDebugPackageForAnalysis(file), {
        onSuccess: (result) => {
            setSummary(result);
            setError(null);
        },
        onError: (err) => {
            setError(err);
        },
    });

    const analyze = useCallback(() => {
        if (selectedFile) {
            setError(null);
            setFileName(selectedFile.name);
            analyzeAsync.execute(selectedFile);
        }
    }, [analyzeAsync, selectedFile]);

    const reset = useCallback(async () => {
        const packageId = summary?.PackageId;

        setSummary(null);
        setFileName(null);
        setSelectedFile(null);
        setError(null);

        if (packageId) {
            try {
                await manageServerService.removeDebugPackageAnalysis(packageId);
            } catch {
                // best-effort cleanup - the server-side report also expires on its own
            }
        }
    }, [manageServerService, summary]);

    // deep-link: load an already-analyzed package by id (valid while the server keeps the report)
    useEffect(() => {
        const { packageId, fileName: fileNameParam } = parseHashQuery();
        if (!packageId) {
            return;
        }

        manageServerService
            .getDebugPackageAnalysisSummary(packageId)
            .then((result) => {
                setSummary(result);
                setFileName(fileNameParam || packageId);
            })
            .catch(() => {
                // the report may have expired server-side - stay on the upload view
            });
    }, [manageServerService]);

    const view: DebugPackageAnalyzerView = summary
        ? "loaded"
        : analyzeAsync.loading
          ? "analyzing"
          : error
            ? "error"
            : "upload";

    return {
        view,
        selectedFile,
        setSelectedFile,
        summary,
        fileName,
        analyze,
        reset,
        isAnalyzing: analyzeAsync.loading,
        error,
    };
}

function parseHashQuery(): { packageId?: string; fileName?: string } {
    const hash = window.location.hash || "";
    const queryIndex = hash.indexOf("?");
    if (queryIndex === -1) {
        return {};
    }

    const params = new URLSearchParams(hash.substring(queryIndex + 1));
    return {
        packageId: params.get("packageId") || undefined,
        fileName: params.get("fileName") || undefined,
    };
}

import { useCallback, useState } from "react";
import { useServices } from "hooks/useServices";
import { useAsync, useAsyncCallback } from "react-async-hook";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

export type DebugPackageAnalyzerView = "upload" | "analyzing" | "error" | "loaded";

export function useDebugPackageAnalyzer() {
    const { manageServerService } = useServices();

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

    const onFileSelected = useCallback(
        (file: File) => {
            if (file) {
                setError(null);
                setFileName(file.name);
                analyzeAsync.execute(file);
            }
        },
        [analyzeAsync]
    );

    const reset = useCallback(async () => {
        const packageId = summary?.PackageId;

        setSummary(null);
        setFileName(null);
        setError(null);

        if (packageId) {
            try {
                await manageServerService.removeDebugPackageAnalysis(packageId);
            } catch {
                // best-effort cleanup - the server-side report also expires on its own
            }
        }
    }, [manageServerService, summary]);

    // deep-link: load an already-analyzed package by id (valid while the server keeps the report).
    // an error (e.g. the report expired server-side) is swallowed by useAsync, leaving us on the upload view.
    useAsync(async () => {
        const { packageId, fileName: fileNameParam } = parseHashQuery();
        if (!packageId) {
            return;
        }

        const result = await manageServerService.getDebugPackageAnalysisSummary(packageId);
        setSummary(result);
        setFileName(fileNameParam || packageId);
    }, []);

    const view: DebugPackageAnalyzerView = summary
        ? "loaded"
        : analyzeAsync.loading
          ? "analyzing"
          : error
            ? "error"
            : "upload";

    return {
        view,
        onFileSelected,
        summary,
        fileName,
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

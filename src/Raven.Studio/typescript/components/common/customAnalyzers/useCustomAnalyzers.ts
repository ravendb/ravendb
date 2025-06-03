import { CustomAnalyzerFormData } from "components/common/customAnalyzers/editCustomAnalyzerValidation";
import { useState } from "react";

export interface CustomAnalyzer extends CustomAnalyzerFormData {
    id: string;
    isSaved: boolean;
}

export function useCustomAnalyzers() {
    const [analyzers, setAnalyzers] = useState<CustomAnalyzer[]>([]);

    const addNewAnalyzer = () => {
        setAnalyzers((prev) => [
            { id: createId(), name: "", code: "", isSaved: false } satisfies CustomAnalyzer,
            ...prev,
        ]);
    };

    const removeAnalyzer = (idx: number) => {
        setAnalyzers((prev) => prev.filter((_, i) => i !== idx));
    };

    const markAsSaved = (idx: number) => {
        setAnalyzers((prev) => prev.map((x, i) => (i === idx ? { ...x, isSaved: true } : x)));
    };

    return {
        analyzers,
        setAnalyzers,
        addNewAnalyzer,
        removeAnalyzer,
        mapFromDto,
        markAsSaved,
    };
}

function mapFromDto(dto: Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition[]): CustomAnalyzer[] {
    return dto.map((x) => ({ id: createId(), code: x.Code, name: x.Name, isSaved: true }) satisfies CustomAnalyzer);
}

function createId() {
    return _.uniqueId("custom-analyzer");
}

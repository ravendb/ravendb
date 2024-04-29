import { CustomAnalyzerFormData } from "components/common/customAnalyzers/editCustomAnalyzerValidation";
import { useState } from "react";

export interface CustomAnalyzer extends CustomAnalyzerFormData {
    id: string;
}

export function useCustomAnalyzers() {
    const [analyzers, setAnalyzers] = useState<CustomAnalyzer[]>([]);

    const addNewAnalyzer = () => {
        setAnalyzers((prev) => [{ id: createId(), name: "", code: "" } satisfies CustomAnalyzer, ...prev]);
    };

    const removeAnalyzer = (idx: number) => {
        setAnalyzers((prev) => prev.filter((_, i) => i !== idx));
    };

    return {
        analyzers,
        setAnalyzers,
        addNewAnalyzer,
        removeAnalyzer,
        mapFromDto,
    };
}

function mapFromDto(dto: Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition[]): CustomAnalyzer[] {
    return dto.map((x) => ({ id: createId(), code: x.Code, name: x.Name }) satisfies CustomAnalyzer);
}

function createId() {
    return _.uniqueId("custom-analyzer");
}

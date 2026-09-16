import { createContext, useContext, type RefObject } from "react";
import type ReactAce from "react-ace";

type AceEditorContextValue = {
    aceRef: RefObject<ReactAce | null>;
    rootRef: RefObject<HTMLDivElement | null>;
    setHeight: (height: number) => void;
};

const AceEditorContext = createContext<AceEditorContextValue | null>(null);

export function useAceEditorContext() {
    const context = useContext(AceEditorContext);

    if (!context) {
        throw new Error("AceEditor actions must be rendered inside AceEditor.");
    }

    return context;
}

export default AceEditorContext;

import { createContext, RefObject, useContext } from "react";
import ReactAce from "react-ace";

export interface AceEditorContextValue {
    aceRef?: RefObject<ReactAce>;
    setHeight: (height: number) => void;
}

const AceEditorContext = createContext<AceEditorContextValue>(null);

export function useAceEditorContext() {
    const context = useContext(AceEditorContext);

    if (!context) {
        throw new Error(
            "You need to provide aceRef and AceEditor.* component must be rendered as child of AceEditor component."
        );
    }

    return context;
}

export default AceEditorContext;

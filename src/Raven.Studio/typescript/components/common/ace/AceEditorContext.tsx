import { createContext, RefObject, useContext } from "react";
import ReactAce from "react-ace";

const AceEditorContext = createContext<RefObject<ReactAce>>(null);

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

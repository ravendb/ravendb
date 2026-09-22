import getDocumentsPreviewCommand from "commands/database/documents/getDocumentsPreviewCommand";
import document from "models/database/documents/document";

// The documents preview endpoint replaces nested objects and arrays with stubs and trims long strings,
// the affected property names are kept in the metadata so the full value can be fetched on demand.
export function hasIncompletePreviewValue(doc: document, property: string): boolean {
    const metadata = doc.__metadata as unknown as Record<string, unknown>;

    const objectStubs = metadata[getDocumentsPreviewCommand.ObjectStubsKey] as Record<string, number> | undefined;
    if (objectStubs && property in objectStubs) {
        return true;
    }

    const arrayStubs = metadata[getDocumentsPreviewCommand.ArrayStubsKey] as Record<string, number> | undefined;
    if (arrayStubs && property in arrayStubs) {
        return true;
    }

    const trimmedValues = metadata[getDocumentsPreviewCommand.TrimmedValueKey] as string[] | undefined;
    return !!trimmedValues && trimmedValues.includes(property);
}

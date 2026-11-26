const STORAGE_KEY = "raven-documents-preview";

export const documentPreviewStorage = {
    isEnabled(): boolean {
        return localStorage.getItem(STORAGE_KEY) !== "false";
    },
    save(isEnabled: boolean) {
        localStorage.setItem(STORAGE_KEY, String(isEnabled));
    },
};

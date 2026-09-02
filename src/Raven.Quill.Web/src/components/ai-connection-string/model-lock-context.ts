import { createContext, useContext } from "react";

/**
 * Whether the fields that decide the shape of generated embeddings are frozen for this form.
 *
 * RavenDB rejects such a change while a GenAI or embeddings task uses the connection string,
 * because the stored embeddings would no longer match the ones the task produces from then on
 * (PutAiConnectionStringCommand.InClusterValidation, AiSettingsCompareDifferences
 * .RequiresEmbeddingsRegeneration). That check only guards per-database connection strings —
 * the server-wide write Quill uses goes straight through — so this lock is what protects them.
 *
 * The provider fields read it here rather than take it as a prop, so only the fields the rule
 * actually covers care about it. Outside a provider nothing is locked.
 */
const ModelLockContext = createContext(false);

export function useIsModelLocked() {
    return useContext(ModelLockContext);
}

export default ModelLockContext;

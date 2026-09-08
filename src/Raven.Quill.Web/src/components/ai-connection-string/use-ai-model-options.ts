import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { AiModelsRequest } from "@/api/generated/server-api";
import { useDebouncedValue } from "@/hooks/use-debounced-value";

const DEBOUNCE_MS = 300;

/**
 * Fetches model suggestions for the provider settings currently entered in the form,
 * mirroring Studio's model autocomplete. Pass `null` while the settings are incomplete
 * (e.g. no API key yet) to skip fetching. Errors just yield no suggestions — the model
 * field stays free-text either way.
 */
export function useAiModelOptions(request: AiModelsRequest | null): string[] {
    // React Compiler memoizes the caller's `request` literal, so debouncing by reference
    // waits until the user stops typing instead of firing per keystroke.
    const debouncedRequest = useDebouncedValue(request, DEBOUNCE_MS);

    const { data } = useQuery({
        ...api.queries.aiModels.list(debouncedRequest ?? {}),
        enabled: debouncedRequest !== null,
        select: (response) => [...response.models].sort(),
    });

    return data ?? [];
}

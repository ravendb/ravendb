import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { useWatch, type UseFormReturn } from "react-hook-form";
import { api } from "@/api/api";
import type { AiModelType } from "@/api/generated/server-api";
import {
    computeConnectionTestKey,
    mapFormDataToDto,
    type ConnectionStringFormData,
} from "@/components/ai-connection-string/ai-connection-string-utils";

const DEFAULT_TEST_ERROR = "The connection could not be verified.";

type TestAttempt = {
    key: string;
    error: string | null;
};

/** A failed test that the connection test UI already reports, so callers must not report it again. */
export class ConnectionTestFailedError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "ConnectionTestFailedError";
    }
}

function getTestKey(values: ConnectionStringFormData): string {
    return computeConnectionTestKey(values.provider, values[values.provider]);
}

async function getTestError(values: ConnectionStringFormData, modelType: AiModelType): Promise<string | null> {
    try {
        const result = await api.services.aiConnectionStrings.test(mapFormDataToDto(values, modelType));
        return result.success ? null : (result.error ?? DEFAULT_TEST_ERROR);
    } catch (error) {
        return error instanceof Error ? error.message : DEFAULT_TEST_ERROR;
    }
}

export function useAiConnectionTest(modelType: AiModelType, form: UseFormReturn<ConnectionStringFormData>) {
    const { control, getValues, trigger } = form;
    const [attempt, setAttempt] = useState<TestAttempt | null>(null);

    // The key must be derived from the watched values (not getValues), otherwise React Compiler
    // memoizes it against the stable getValues reference and it never reflects form changes.
    const provider = useWatch({ control, name: "provider" });
    const settings = useWatch({ control, name: provider });
    const testKey = computeConnectionTestKey(provider, settings);

    // Keying the attempt drops the outcome as soon as the operator edits anything it was based on.
    const isVerified = attempt?.key === testKey && attempt.error === null;

    const testMutation = useMutation({
        mutationFn: async (values: ConnectionStringFormData) => {
            const error = await getTestError(values, modelType);
            setAttempt({ key: getTestKey(values), error });
            return error;
        },
    });

    return {
        isVerified,
        isPending: testMutation.isPending,
        error: attempt?.key === testKey ? attempt.error : null,
        test: async () => {
            if (await trigger([provider], { shouldFocus: true })) {
                await testMutation.mutateAsync(getValues());
            }
        },
        /** Saving requires a verified connection; an already verified one is not tested twice. */
        ensureVerified: async (values: ConnectionStringFormData) => {
            if (isVerified) {
                return;
            }

            const error = await testMutation.mutateAsync(values);
            if (error) {
                throw new ConnectionTestFailedError(error);
            }
        },
    };
}

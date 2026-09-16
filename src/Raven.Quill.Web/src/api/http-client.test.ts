import type { ApiErrorResponse } from "@/api/generated/server-api";
import { createApiClient, isApiError, type ApiTransport } from "@/api/http-client";
import { describe, expect, it } from "vitest";

function respondWith(body: unknown, status = 400): ApiTransport {
    return () =>
        Promise.resolve(
            new Response(JSON.stringify(body), {
                status,
                headers: { "Content-Type": "application/json" },
            }),
        );
}

async function postAndCatch(body: unknown) {
    const client = createApiClient({ transport: respondWith(body) });

    try {
        await client.post<never, ApiErrorResponse>("/setup/map", {});
    } catch (error) {
        return isApiError<ApiErrorResponse>(error) ? error : null;
    }

    return null;
}

const MAP_ERRORS = [
    "Table 'Customer': property name 'LanguageId' from linked table 'Language' conflicts with a column mapping or another embedded/linked table",
    "Table 'Customer': property name 'CurrencyId' from linked table 'Currency' conflicts with a column mapping or another embedded/linked table",
];

describe("api error message", () => {
    it("surfaces the errors list when error is null", async () => {
        const error = await postAndCatch({ error: null, errors: MAP_ERRORS, code: null });

        expect(error?.message).toBe(MAP_ERRORS.join("\n"));
    });

    it("keeps the parsed body so callers can read the list", async () => {
        const error = await postAndCatch({ error: null, errors: MAP_ERRORS, code: null });

        expect(error?.status).toBe(400);
        expect(error?.details?.errors).toEqual(MAP_ERRORS);
    });

    it("prefers an explicit error over the list", async () => {
        const error = await postAndCatch({ error: "Slug is already taken", errors: MAP_ERRORS, code: null });

        expect(error?.message).toBe("Slug is already taken");
    });

    it("falls back to the status when the list is empty", async () => {
        const error = await postAndCatch({ error: null, errors: [], code: null });

        expect(error?.message).toBe("Request failed with 400");
    });

    it("falls back to the status when errors is not a list of strings", async () => {
        const error = await postAndCatch({ errors: { slug: ["is required"] } });

        expect(error?.message).toBe("Request failed with 400");
    });
});

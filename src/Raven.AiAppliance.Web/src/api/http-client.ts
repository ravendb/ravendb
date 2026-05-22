export type ApiRequestOptions = Omit<RequestInit, "body"> & {
    body?: unknown;
    responseType?: ApiResponseType;
    searchParams?: Record<string, boolean | number | string | undefined>;
};

export type ApiResponseType = "arrayBuffer" | "auto" | "blob" | "json" | "response" | "text" | "void";

export type ApiTransport = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

export type ApiClientOptions = {
    baseUrl?: string;
    transport?: ApiTransport;
};

export class ApiError extends Error {
    readonly status: number;
    readonly details: unknown;

    constructor(message: string, status: number, details: unknown) {
        super(message);
        this.name = "ApiError";
        this.status = status;
        this.details = details;
    }
}

export function createApiClient({ baseUrl = "/api", transport = fetch }: ApiClientOptions = {}) {
    const normalizedBaseUrl = baseUrl.replace(/\/+$/, "");

    async function request<TResponse>(
        path: string,
        { body, headers, responseType = "auto", searchParams, ...init }: ApiRequestOptions = {},
    ): Promise<TResponse> {
        const url = createUrl(normalizedBaseUrl, path, searchParams);
        const requestHeaders = new Headers(headers);
        const { contentType, serializedBody } = serializeRequestBody(body);

        if (contentType && !requestHeaders.has("Content-Type")) {
            requestHeaders.set("Content-Type", contentType);
        }

        const response = await transport(url, {
            credentials: "same-origin",
            ...init,
            headers: requestHeaders,
            body: serializedBody,
        });

        if (!response.ok) {
            throw await createApiError(response);
        }

        return await parseResponse<TResponse>(response, responseType, init.method);
    }

    return {
        delete: <TResponse>(path: string, options?: ApiRequestOptions) =>
            request<TResponse>(path, { ...options, method: "DELETE" }),
        get: <TResponse>(path: string, options?: ApiRequestOptions) =>
            request<TResponse>(path, { ...options, method: "GET" }),
        patch: <TResponse>(path: string, body?: unknown, options?: ApiRequestOptions) =>
            request<TResponse>(path, { ...options, body, method: "PATCH" }),
        post: <TResponse>(path: string, body?: unknown, options?: ApiRequestOptions) =>
            request<TResponse>(path, { ...options, body, method: "POST" }),
        put: <TResponse>(path: string, body?: unknown, options?: ApiRequestOptions) =>
            request<TResponse>(path, { ...options, body, method: "PUT" }),
    };
}

export type ApiClient = ReturnType<typeof createApiClient>;

async function createApiError(response: Response) {
    const details = await readErrorDetails(response);
    const message = getErrorMessage(details) ?? `Request failed with ${response.status}`;

    return new ApiError(message, response.status, details);
}

function getErrorMessage(details: unknown) {
    if (typeof details !== "object" || details === null) {
        return undefined;
    }

    if ("message" in details && typeof details.message === "string") {
        return details.message;
    }

    if ("detail" in details && typeof details.detail === "string") {
        return details.detail;
    }

    if ("error" in details && typeof details.error === "string") {
        return details.error;
    }

    return undefined;
}

async function readErrorDetails(response: Response) {
    if (isEmptyResponse(response)) {
        return undefined;
    }

    const contentType = response.headers.get("Content-Type") ?? "";
    const rawBody = await response.text();

    if (rawBody === "") {
        return undefined;
    }

    if (!contentType.includes("application/json")) {
        return rawBody;
    }

    try {
        return JSON.parse(rawBody) as unknown;
    } catch (parseError) {
        return {
            parseError,
            rawBody,
        };
    }
}

function createUrl(baseUrl: string, path: string, searchParams?: ApiRequestOptions["searchParams"]) {
    const normalizedPath = path.startsWith("/") ? path : `/${path}`;
    const url = `${baseUrl}${normalizedPath}`;

    if (!searchParams) {
        return url;
    }

    const [urlWithoutHash, hash = ""] = url.split("#", 2);
    const querySeparator = urlWithoutHash.includes("?") ? "&" : "?";
    const query = new URLSearchParams();

    for (const [key, value] of Object.entries(searchParams)) {
        if (value !== undefined) {
            query.set(key, String(value));
        }
    }

    const queryString = query.toString();

    if (!queryString) {
        return url;
    }

    const hashSuffix = hash ? `#${hash}` : "";

    return `${urlWithoutHash}${querySeparator}${queryString}${hashSuffix}`;
}

function serializeRequestBody(body: unknown) {
    if (body === undefined) {
        return {
            contentType: undefined,
            serializedBody: undefined,
        };
    }

    if (body === null) {
        return {
            contentType: "application/json",
            serializedBody: "null",
        };
    }

    if (typeof body === "string") {
        return {
            contentType: undefined,
            serializedBody: body,
        };
    }

    if (isBodyInit(body)) {
        return {
            contentType: undefined,
            serializedBody: body,
        };
    }

    return {
        contentType: "application/json",
        serializedBody: JSON.stringify(body),
    };
}

function isBodyInit(value: unknown): value is BodyInit {
    if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
        return true;
    }

    if (typeof Blob !== "undefined" && value instanceof Blob) {
        return true;
    }

    if (typeof FormData !== "undefined" && value instanceof FormData) {
        return true;
    }

    if (typeof ReadableStream !== "undefined" && value instanceof ReadableStream) {
        return true;
    }

    if (typeof URLSearchParams !== "undefined" && value instanceof URLSearchParams) {
        return true;
    }

    return false;
}

function isEmptyResponse(response: Response, method?: string) {
    return (
        method?.toUpperCase() === "HEAD" ||
        response.status === 204 ||
        response.status === 205 ||
        response.body === null ||
        response.headers.get("Content-Length") === "0"
    );
}

async function parseResponse<TResponse>(
    response: Response,
    responseType: ApiResponseType,
    method?: string,
): Promise<TResponse> {
    if (responseType === "response") {
        return response as TResponse;
    }

    if (responseType === "void" || isEmptyResponse(response, method)) {
        return undefined as TResponse;
    }

    if (responseType === "arrayBuffer") {
        return (await response.arrayBuffer()) as TResponse;
    }

    if (responseType === "blob") {
        return (await response.blob()) as TResponse;
    }

    if (responseType === "json") {
        return (await response.json()) as TResponse;
    }

    if (responseType === "text") {
        return (await response.text()) as TResponse;
    }

    const contentType = response.headers.get("Content-Type") ?? "";

    if (contentType.includes("application/json")) {
        return (await response.json()) as TResponse;
    }

    if (contentType.startsWith("text/")) {
        return (await response.text()) as TResponse;
    }

    if (!contentType) {
        const text = await response.text();
        return (text === "" ? undefined : text) as TResponse;
    }

    return (await response.blob()) as TResponse;
}

import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

type ExternalConnection = AppFormData["externalConnection"];
/** The chosen source database. The form's provider field also allows "" while unselected. */
export type Provider = Exclude<ExternalConnection["provider"], "">;

export type SslMode = ExternalConnection["fields"]["ssl"];

export type ConnectionValues = {
    host: string;
    port: number | null;
    database: string;
    username: string;
    password: string;
    ssl: SslMode;
};

export type ParsedConnectionString = {
    values: ConnectionValues;
    droppedKeywords: string[];
    /** False when no keyword mapped to a connection value - the values are then all defaults. */
    hasRecognizedKeywords: boolean;
};

export const DEFAULT_PROVIDER: Provider = "Npgsql";

export const DEFAULT_PORT_BY_PROVIDER: Record<Provider, number> = {
    Npgsql: 5432,
    SqlClient: 1433,
    MySqlConnectorFactory: 3306,
};

const KEYWORDS_BY_PROVIDER: Record<
    Provider,
    { host: string; port: string | null; database: string; username: string; password: string }
> = {
    Npgsql: { host: "Host", port: "Port", database: "Database", username: "Username", password: "Password" },
    SqlClient: { host: "Server", port: null, database: "Database", username: "User ID", password: "Password" },
    MySqlConnectorFactory: {
        host: "Server",
        port: "Port",
        database: "Database",
        username: "User ID",
        password: "Password",
    },
};

const SSL_BY_PROVIDER: Record<Provider, { keyword: string; require: string; disable: string }> = {
    Npgsql: { keyword: "SSL Mode", require: "Require", disable: "Disable" },
    SqlClient: { keyword: "Encrypt", require: "True", disable: "False" },
    MySqlConnectorFactory: { keyword: "SslMode", require: "Required", disable: "None" },
};

export function buildConnectionString(provider: Provider | "", values: ConnectionValues): string {
    // No provider chosen yet - there is no dialect to build a string in.
    if (provider === "") {
        return "";
    }

    const keywords = KEYWORDS_BY_PROVIDER[provider];
    const host = values.host.trim();
    const hostValue = provider === "SqlClient" && values.port != null ? `${host},${values.port}` : host;
    const pairs: string[] = [];

    if (hostValue !== "") {
        pairs.push(`${keywords.host}=${escapeValue(hostValue)}`);
    }

    if (keywords.port && values.port != null) {
        pairs.push(`${keywords.port}=${values.port}`);
    }

    for (const [keyword, value] of [
        [keywords.database, values.database.trim()],
        [keywords.username, values.username.trim()],
        [keywords.password, values.password],
    ]) {
        if (value !== "") {
            pairs.push(`${keyword}=${escapeValue(value)}`);
        }
    }

    // Empty details produce an empty string - an SSL keyword alone is never a usable connection.
    if (pairs.length === 0) {
        return "";
    }

    if (values.ssl !== "default") {
        const ssl = SSL_BY_PROVIDER[provider];
        pairs.push(`${ssl.keyword}=${values.ssl === "require" ? ssl.require : ssl.disable}`);
    }

    return pairs.join(";");
}

export function resolveConnectionString(
    connection: Pick<ExternalConnection, "provider" | "mode" | "fields" | "connectionString">,
): string {
    return connection.mode === "raw"
        ? connection.connectionString.trim()
        : buildConnectionString(connection.provider, connection.fields);
}

function escapeValue(value: string): string {
    if (value !== "" && !/[;'"=\r\n]/.test(value) && value === value.trim()) {
        return value;
    }

    return `'${value.replaceAll("'", "''")}'`;
}

const FIELD_BY_KEYWORD: Record<string, keyof ConnectionValues> = {
    host: "host",
    server: "host",
    "data source": "host",
    address: "host",
    addr: "host",
    "network address": "host",
    port: "port",
    database: "database",
    "initial catalog": "database",
    username: "username",
    "user name": "username",
    "user id": "username",
    user: "username",
    uid: "username",
    password: "password",
    pwd: "password",
    "ssl mode": "ssl",
    sslmode: "ssl",
    encrypt: "ssl",
};

const DISABLE_SSL_VALUES = new Set(["disable", "disabled", "none", "false", "0", "no"]);

// "prefer"/"preferred" are what Npgsql/MySqlConnector do anyway when the keyword is absent.
const DEFAULT_SSL_VALUES = new Set(["prefer", "preferred"]);

function toSslMode(value: string): SslMode {
    const normalized = value.trim().toLowerCase();

    if (DISABLE_SSL_VALUES.has(normalized)) {
        return "disable";
    }

    if (DEFAULT_SSL_VALUES.has(normalized)) {
        return "default";
    }

    return "require";
}

export function parseConnectionString(connectionString: string): ParsedConnectionString {
    const values: ConnectionValues = {
        host: "",
        port: null,
        database: "",
        username: "",
        password: "",
        ssl: "default",
    };
    const droppedKeywords: string[] = [];
    let hasRecognizedKeywords = false;

    for (const segment of splitSegments(connectionString)) {
        const separatorIndex = segment.indexOf("=");

        if (separatorIndex < 0) {
            droppedKeywords.push(segment);
            continue;
        }

        const keyword = segment.slice(0, separatorIndex).trim();
        const value = unquote(segment.slice(separatorIndex + 1).trim());
        const field = FIELD_BY_KEYWORD[keyword.toLowerCase()];

        if (!field) {
            droppedKeywords.push(keyword);
            continue;
        }

        hasRecognizedKeywords = true;

        if (field === "port") {
            values.port = parsePort(value);
        } else if (field === "ssl") {
            values.ssl = toSslMode(value);
        } else if (field === "host") {
            const { host, port } = splitHostAndPort(value);
            values.host = host;
            values.port = port ?? values.port;
        } else {
            values[field] = value;
        }
    }

    return { values, droppedKeywords, hasRecognizedKeywords };
}

function splitHostAndPort(value: string): { host: string; port: number | null } {
    const commaIndex = value.lastIndexOf(",");

    if (commaIndex < 0) {
        return { host: value, port: null };
    }

    const port = parsePort(value.slice(commaIndex + 1).trim());

    return port === null ? { host: value, port: null } : { host: value.slice(0, commaIndex).trim(), port };
}

function parsePort(value: string): number | null {
    return /^\d+$/.test(value) ? Number(value) : null;
}

function splitSegments(connectionString: string): string[] {
    const segments: string[] = [];
    let current = "";
    let quote: string | null = null;

    for (let i = 0; i < connectionString.length; i++) {
        const char = connectionString[i];

        if (quote !== null) {
            if (char === quote && connectionString[i + 1] === quote) {
                current += char + char;
                i++;
            } else {
                if (char === quote) {
                    quote = null;
                }
                current += char;
            }
        } else if (char === "'" || char === '"') {
            quote = char;
            current += char;
        } else if (char === ";") {
            segments.push(current);
            current = "";
        } else {
            current += char;
        }
    }

    segments.push(current);

    return segments.map((segment) => segment.trim()).filter(Boolean);
}

function unquote(value: string): string {
    const first = value[0];

    if ((first === "'" || first === '"') && value.length >= 2 && value.endsWith(first)) {
        return value.slice(1, -1).replaceAll(first + first, first);
    }

    return value;
}

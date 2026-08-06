import { describe, expect, it } from "vitest";
import {
    buildConnectionString,
    parseConnectionString,
    resolveConnectionString,
    type ConnectionValues,
} from "@/pages/setup/add-app-wizard/connection-string";

function values(overrides: Partial<ConnectionValues> = {}): ConnectionValues {
    return {
        host: "localhost",
        port: 5432,
        database: "acme_shop",
        username: "admin",
        password: "secret",
        isSecured: false,
        ...overrides,
    };
}

function lines(...pairs: string[]): string {
    return pairs.map((pair) => `${pair};`).join("\n");
}

describe("buildConnectionString", () => {
    it("builds a single-line PostgreSQL connection string", () => {
        expect(buildConnectionString("Npgsql", values())).toBe(
            "Host=localhost;Port=5432;Database=acme_shop;Username=admin;Password=secret;SSL Mode=Disable",
        );
    });

    it("builds a MySQL connection string with the User ID keyword", () => {
        expect(buildConnectionString("MySqlConnectorFactory", values({ port: 3306 }))).toBe(
            "Server=localhost;Port=3306;Database=acme_shop;User ID=admin;Password=secret;SslMode=None",
        );
    });

    it("folds the SQL Server port into the server value", () => {
        expect(buildConnectionString("SqlClient", values({ port: 1433 }))).toBe(
            "Server=localhost,1433;Database=acme_shop;User ID=admin;Password=secret;Encrypt=False",
        );
    });

    it("asks for an encrypted transport with the keyword each provider expects", () => {
        expect(buildConnectionString("Npgsql", values({ isSecured: true }))).toContain("SSL Mode=Require");
        expect(buildConnectionString("SqlClient", values({ isSecured: true }))).toContain("Encrypt=True");
        expect(buildConnectionString("MySqlConnectorFactory", values({ isSecured: true }))).toContain(
            "SslMode=Required",
        );
    });

    it("omits the port and password when not provided", () => {
        expect(buildConnectionString("Npgsql", values({ port: null, password: "" }))).toBe(
            "Host=localhost;Database=acme_shop;Username=admin;SSL Mode=Disable",
        );
        expect(buildConnectionString("SqlClient", values({ port: null }))).toBe(
            "Server=localhost;Database=acme_shop;User ID=admin;Password=secret;Encrypt=False",
        );
    });

    it("returns an empty string when every field is empty", () => {
        const empty = values({ host: "", port: null, database: "", username: "", password: "" });

        expect(buildConnectionString("Npgsql", empty)).toBe("");
        expect(buildConnectionString("Npgsql", { ...empty, isSecured: true })).toBe("");
    });

    it("quotes values containing separators or quotes", () => {
        expect(buildConnectionString("Npgsql", values({ password: "p;a='s" }))).toContain("Password='p;a=''s'");
    });

    it("quotes values with surrounding whitespace", () => {
        expect(buildConnectionString("Npgsql", values({ password: " secret " }))).toContain("Password=' secret '");
    });
});

describe("parseConnectionString", () => {
    it("parses a PostgreSQL connection string", () => {
        expect(
            parseConnectionString(
                "Npgsql",
                "Host=localhost;Port=5432;Database=acme_shop;Username=admin;Password=secret;SSL Mode=Disable",
            ),
        ).toEqual({ values: values(), droppedKeywords: [], hasRecognizedKeywords: true });
    });

    it("parses a connection string with one keyword per line", () => {
        expect(
            parseConnectionString(
                "Npgsql",
                lines(
                    "Host=localhost",
                    "Port=5432",
                    "Database=acme_shop",
                    "Username=admin",
                    "Password=secret",
                    "SSL Mode=Disable",
                ),
            ),
        ).toEqual({ values: values(), droppedKeywords: [], hasRecognizedKeywords: true });
    });

    it("parses keyword aliases case-insensitively", () => {
        expect(
            parseConnectionString(
                "MySqlConnectorFactory",
                "server=db.example.com;PORT=3306;Initial Catalog=shop;Uid=root;Pwd=secret;sslmode=none",
            ).values,
        ).toEqual(values({ host: "db.example.com", port: 3306, database: "shop", username: "root" }));
    });

    it("extracts the SQL Server port from the server value", () => {
        expect(
            parseConnectionString(
                "SqlClient",
                "Server=db.example.com,1433;Database=shop;User ID=sa;Password=secret;Encrypt=False",
            ).values,
        ).toEqual(values({ host: "db.example.com", port: 1433, database: "shop", username: "sa" }));
    });

    it("splits the host and port whichever provider's dialect wrote them", () => {
        const parsed = parseConnectionString(
            "SqlClient",
            "Server=localhost,5432;Database=acme_shop;User ID=admin;Password=secret;Encrypt=True",
        );

        expect(parsed.values).toEqual(values({ isSecured: true }));
        expect(parsed.droppedKeywords).toEqual([]);
    });

    it("leaves a multi-host value alone", () => {
        expect(
            parseConnectionString("Npgsql", "Host=primary.example.com,replica.example.com;Database=shop").values,
        ).toMatchObject({ host: "primary.example.com,replica.example.com", port: null });
    });

    it("reads the secured flag from each provider's SSL keyword", () => {
        expect(parseConnectionString("Npgsql", "Host=h;SSL Mode=Require").values.isSecured).toBe(true);
        expect(parseConnectionString("Npgsql", "Host=h;SSL Mode=Disable").values.isSecured).toBe(false);
        expect(parseConnectionString("SqlClient", "Server=h;Encrypt=True").values.isSecured).toBe(true);
        expect(parseConnectionString("SqlClient", "Server=h;Encrypt=False").values.isSecured).toBe(false);
        expect(parseConnectionString("MySqlConnectorFactory", "Server=h;SslMode=Required").values.isSecured).toBe(true);
        expect(parseConnectionString("MySqlConnectorFactory", "Server=h;SslMode=None").values.isSecured).toBe(false);
    });

    it("reads an opportunistic SSL value as not secured", () => {
        expect(parseConnectionString("Npgsql", "Host=h;SSL Mode=Prefer").values.isSecured).toBe(false);
        expect(parseConnectionString("MySqlConnectorFactory", "Server=h;SslMode=Preferred").values.isSecured).toBe(
            false,
        );
    });

    it("maps a missing SSL keyword to what the driver does by default", () => {
        expect(parseConnectionString("Npgsql", "Host=h;Database=d;Username=u;Password=p").values.isSecured).toBe(false);
        expect(
            parseConnectionString("MySqlConnectorFactory", "Server=h;Database=d;User ID=u;Password=p").values.isSecured,
        ).toBe(false);
        // Microsoft.Data.SqlClient encrypts unless the string says otherwise.
        expect(parseConnectionString("SqlClient", "Server=h;Database=d;User ID=u;Password=p").values.isSecured).toBe(
            true,
        );
    });

    it("keeps a SQL Server named instance intact when there is no port", () => {
        expect(
            parseConnectionString("SqlClient", "Server=host\\SQLEXPRESS;Database=shop;User ID=sa;Password=x").values
                .host,
        ).toBe("host\\SQLEXPRESS");
    });

    it("reports when nothing in the string maps to a connection value", () => {
        const parsed = parseConnectionString("Npgsql", "somevalue");

        expect(parsed.hasRecognizedKeywords).toBe(false);
        expect(parsed.droppedKeywords).toEqual(["somevalue"]);
        expect(parseConnectionString("Npgsql", "Application Name=quill").hasRecognizedKeywords).toBe(false);
        expect(parseConnectionString("Npgsql", "Host=h").hasRecognizedKeywords).toBe(true);
    });

    it("names the keywords no input represents", () => {
        expect(
            parseConnectionString(
                "SqlClient",
                "Server=localhost;Database=shop;User ID=sa;Password=x;TrustServerCertificate=True;Connect Timeout=30",
            ).droppedKeywords,
        ).toEqual(["TrustServerCertificate", "Connect Timeout"]);
    });

    it("drops nothing from a string the values fully express", () => {
        expect(
            parseConnectionString("Npgsql", "Host=h;Port=5432;Database=d;Username=u;Password=p;SSL Mode=Require")
                .droppedKeywords,
        ).toEqual([]);
    });

    it("unquotes values containing separators", () => {
        expect(
            parseConnectionString("Npgsql", "Host=localhost;Database=shop;Username=admin;Password='p;a=''s'").values
                .password,
        ).toBe("p;a='s");
    });

    it("round-trips through build for every provider", () => {
        for (const provider of ["Npgsql", "SqlClient", "MySqlConnectorFactory"] as const) {
            for (const isSecured of [true, false]) {
                const original = values({ password: "we;ird'pa=ss", isSecured });

                expect(parseConnectionString(provider, buildConnectionString(provider, original))).toEqual({
                    values: original,
                    droppedKeywords: [],
                    hasRecognizedKeywords: true,
                });
            }
        }
    });
});

describe("resolveConnectionString", () => {
    it("composes the fields while the fields editor is active", () => {
        expect(
            resolveConnectionString({
                provider: "Npgsql",
                mode: "fields",
                fields: values(),
                connectionString: "Host=ignored",
            }),
        ).toBe("Host=localhost;Port=5432;Database=acme_shop;Username=admin;Password=secret;SSL Mode=Disable");
    });

    it("uses the pasted string verbatim while the raw editor is active", () => {
        expect(
            resolveConnectionString({
                provider: "SqlClient",
                mode: "raw",
                fields: values(),
                connectionString: "  Server=host\\SQLEXPRESS;Integrated Security=true  ",
            }),
        ).toBe("Server=host\\SQLEXPRESS;Integrated Security=true");
    });
});

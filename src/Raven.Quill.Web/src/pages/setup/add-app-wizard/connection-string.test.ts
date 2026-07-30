import { describe, expect, it } from "vitest";
import {
    buildConnectionString,
    getPortAfterProviderChange,
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

describe("buildConnectionString", () => {
    it("builds a PostgreSQL connection string", () => {
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

    it("leaves out empty values instead of emitting a bare keyword", () => {
        expect(buildConnectionString("Npgsql", values({ host: "", database: "", username: "", password: "" }))).toBe(
            "Port=5432;SSL Mode=Disable",
        );
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
                "Host=localhost;Port=5432;Database=acme_shop;Username=admin;Password=secret;SSL Mode=Disable",
            ),
        ).toEqual({ values: values(), droppedKeywords: [] });
    });

    it("parses keyword aliases case-insensitively", () => {
        expect(
            parseConnectionString(
                "server=db.example.com;PORT=3306;Initial Catalog=shop;Uid=root;Pwd=secret;sslmode=none",
            ).values,
        ).toEqual(values({ host: "db.example.com", port: 3306, database: "shop", username: "root" }));
    });

    it("extracts the SQL Server port from the server value", () => {
        expect(
            parseConnectionString("Server=db.example.com,1433;Database=shop;User ID=sa;Password=secret;Encrypt=False")
                .values,
        ).toEqual(values({ host: "db.example.com", port: 1433, database: "shop", username: "sa" }));
    });

    it("splits the host and port whichever provider's dialect wrote them", () => {
        const parsed = parseConnectionString(
            "Server=localhost,5432;Database=acme_shop;User ID=admin;Password=secret;Encrypt=True",
        );

        expect(parsed.values).toEqual(values({ isSecured: true }));
        expect(parsed.droppedKeywords).toEqual([]);
    });

    it("leaves a multi-host value alone", () => {
        expect(
            parseConnectionString("Host=primary.example.com,replica.example.com;Database=shop").values,
        ).toMatchObject({ host: "primary.example.com,replica.example.com", port: null });
    });

    it("reads the secured flag from each provider's SSL keyword", () => {
        expect(parseConnectionString("Host=h;SSL Mode=Require").values.isSecured).toBe(true);
        expect(parseConnectionString("Host=h;SSL Mode=Disable").values.isSecured).toBe(false);
        expect(parseConnectionString("Server=h;Encrypt=True").values.isSecured).toBe(true);
        expect(parseConnectionString("Server=h;Encrypt=False").values.isSecured).toBe(false);
        expect(parseConnectionString("Server=h;SslMode=Required").values.isSecured).toBe(true);
        expect(parseConnectionString("Server=h;SslMode=None").values.isSecured).toBe(false);
    });

    it("treats a missing SSL keyword as secured", () => {
        expect(parseConnectionString("Host=h;Database=d;Username=u;Password=p").values.isSecured).toBe(true);
    });

    it("keeps a SQL Server named instance intact when there is no port", () => {
        expect(parseConnectionString("Server=host\\SQLEXPRESS;Database=shop;User ID=sa;Password=x").values.host).toBe(
            "host\\SQLEXPRESS",
        );
    });

    it("names the keywords no input represents", () => {
        expect(
            parseConnectionString(
                "Server=localhost;Database=shop;User ID=sa;Password=x;TrustServerCertificate=True;Connect Timeout=30",
            ).droppedKeywords,
        ).toEqual(["TrustServerCertificate", "Connect Timeout"]);
    });

    it("drops nothing from a string the values fully express", () => {
        expect(
            parseConnectionString("Host=h;Port=5432;Database=d;Username=u;Password=p;SSL Mode=Require").droppedKeywords,
        ).toEqual([]);
    });

    it("unquotes values containing separators", () => {
        expect(
            parseConnectionString("Host=localhost;Database=shop;Username=admin;Password='p;a=''s'").values.password,
        ).toBe("p;a='s");
    });

    it("round-trips through build for every provider", () => {
        const original = values({ password: "we;ird'pa=ss", isSecured: true });

        for (const provider of ["Npgsql", "SqlClient", "MySqlConnectorFactory"] as const) {
            expect(parseConnectionString(buildConnectionString(provider, original))).toEqual({
                values: original,
                droppedKeywords: [],
            });
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

describe("getPortAfterProviderChange", () => {
    it("follows the new provider when the port is still the previous default", () => {
        expect(getPortAfterProviderChange(5432, "Npgsql", "SqlClient")).toBe(1433);
        expect(getPortAfterProviderChange(1433, "SqlClient", "MySqlConnectorFactory")).toBe(3306);
        expect(getPortAfterProviderChange(3306, "MySqlConnectorFactory", "Npgsql")).toBe(5432);
    });

    it("fills the new provider's default when the port is empty", () => {
        expect(getPortAfterProviderChange(null, "Npgsql", "MySqlConnectorFactory")).toBe(3306);
    });

    it("keeps a port the operator chose", () => {
        expect(getPortAfterProviderChange(6543, "Npgsql", "SqlClient")).toBe(6543);
    });
});

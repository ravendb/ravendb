import { delay, http, HttpResponse } from "msw";
import type {
    CdcColumnMapping,
    CdcSinkEmbeddedTableConfig,
    CdcSinkLinkedTableConfig,
    CdcSinkTableConfig,
    DiscoverResponse,
    DroppedTable,
    MigrationApplyResponse,
    MigrationFrame,
    ProposedCollection,
} from "@/api/generated/server-api";
import { apiHttp } from "@/mocks/api-http";
import { discoveredTable } from "@/mocks/discovery-fixtures";

type StreamOptions = {
    frameDelayMs?: number;
    /** Keeps the stream open after the last frame, so the planner stays at work. */
    isEndless?: boolean;
};

// The planner streams its frames as NDJSON, so the two streaming endpoints use plain msw instead of
// `apiHttp` - mirroring assistantMocks.chat.
export const migrationMocks = {
    start: (frames: MigrationFrame[] = plannerOpeningFrames, options?: StreamOptions) =>
        streamFrames("/api/setup/migration/start", frames, options),
    ask: (frames: MigrationFrame[] = plannerBuildFrames, options?: StreamOptions) =>
        streamFrames("/api/setup/migration/ask", frames, options),
    apply: (response: MigrationApplyResponse = sampleMigrationApply) =>
        apiHttp.post("/api/setup/migration/apply", ({ response: res }) => res(200).json(response)),
    /** The assembled plan does not validate as a whole, which the server answers with 422 and the reasons. */
    applyRejected: (errors: string[]) =>
        apiHttp.post("/api/setup/migration/apply", ({ response }) => response(422).json({ errors })),
    remove: () => apiHttp.post("/api/setup/migration/remove", ({ response }) => response(204).empty()),
};

function streamFrames(
    path: string,
    frames: MigrationFrame[],
    { frameDelayMs = 400, isEndless = false }: StreamOptions = {},
) {
    return http.post(path, () => {
        const encoder = new TextEncoder();
        const stream = new ReadableStream<Uint8Array>({
            async start(controller) {
                for (const frame of frames) {
                    await delay(frameDelayMs);
                    controller.enqueue(encoder.encode(`${JSON.stringify(frame)}\n`));
                }

                if (isEndless) {
                    await delay("infinite");
                }

                controller.close();
            },
        });

        return new HttpResponse(stream, { headers: { "Content-Type": "application/x-ndjson" } });
    });
}

// The planner registers configurations as the server serializes them, every property present.
export function columnMapping(column: string, name: string): CdcColumnMapping {
    return { column, name, type: "Default" };
}

export function rootTableConfig(
    config: Pick<CdcSinkTableConfig, "collectionName" | "sourceTableSchema" | "sourceTableName" | "columns"> &
        Partial<CdcSinkTableConfig>,
): CdcSinkTableConfig {
    return {
        primaryKeyColumns: ["Id"],
        patch: null,
        onDelete: { ignoreDeletes: false, patch: null },
        disabled: false,
        embeddedTables: [],
        linkedTables: [],
        ...config,
    };
}

export function embeddedTableConfig(
    config: Pick<
        CdcSinkEmbeddedTableConfig,
        "sourceTableSchema" | "sourceTableName" | "propertyName" | "joinColumns" | "columns"
    > &
        Partial<CdcSinkEmbeddedTableConfig>,
): CdcSinkEmbeddedTableConfig {
    return {
        primaryKeyColumns: ["Id"],
        type: "Array",
        patch: null,
        onDelete: { ignoreDeletes: false, patch: null },
        caseSensitiveKeys: false,
        embeddedTables: [],
        linkedTables: [],
        ...config,
    };
}

function linkedTableConfig(
    schema: string,
    collection: string,
    propertyName: string,
    joinColumn: string,
): CdcSinkLinkedTableConfig {
    return {
        sourceTableSchema: schema,
        sourceTableName: collection,
        propertyName,
        linkedCollectionName: collection,
        joinColumns: [joinColumn],
    };
}

export function proposedCollection(
    collection: string,
    rootTable: string,
    absorbs: { table: string; how: "Embed" | "Link"; why?: string }[],
    why: string,
): ProposedCollection {
    return {
        collection,
        rootTable,
        absorbs: absorbs.map(({ table, how, why: absorbedWhy }) => ({ table, how, why: absorbedWhy ?? null })),
        why,
    };
}

export function droppedTable(table: string, why: string): DroppedTable {
    return { table, why };
}

/** The demo_shop sample the planner designs are drawn on. */
export const plannerDiscovery: DiscoverResponse = {
    catalogName: "demo_shop",
    success: true,
    hasPermissionToSetup: true,
    errors: [],
    warnings: [],
    tables: [
        discoveredTable("dbo", "Customers", [
            ["Id", "int"],
            ["Name", "nvarchar(200)"],
            ["Email", "nvarchar(320)"],
        ]),
        discoveredTable(
            "dbo",
            "CustomerAddresses",
            [
                ["Id", "int"],
                ["CustomerId", "int"],
                ["City", "nvarchar(100)"],
                ["IsDefault", "bit"],
            ],
            { CustomerId: "Customers" },
        ),
        discoveredTable(
            "dbo",
            "Orders",
            [
                ["Id", "int"],
                ["CustomerId", "int"],
                ["PlacedAt", "datetime2"],
                ["Total", "decimal(18,2)"],
            ],
            { CustomerId: "Customers" },
        ),
        discoveredTable(
            "dbo",
            "OrderLines",
            [
                ["Id", "int"],
                ["OrderId", "int"],
                ["ProductId", "int"],
                ["Quantity", "int"],
            ],
            { OrderId: "Orders", ProductId: "Products" },
        ),
        discoveredTable("dbo", "Products", [
            ["Id", "int"],
            ["Name", "nvarchar(200)"],
            ["Price", "decimal(18,2)"],
        ]),
        discoveredTable("dbo", "AuditLog", [
            ["Id", "int"],
            ["Entry", "nvarchar(max)"],
            ["CreatedAt", "datetime2"],
        ]),
    ],
};

/** A Postgres database with every selected table in one schema, enough of them to fold the list away. */
export const loanTablesDiscovery: DiscoverResponse = {
    catalogName: null,
    success: true,
    hasPermissionToSetup: true,
    errors: [],
    warnings: [],
    tables: [
        "acc_gl_account",
        "acc_gl_closure",
        "acc_gl_journal_entry",
        "batch_job_execution",
        "batch_step_execution",
        "m_address",
        "m_appuser",
        "m_appuser_role",
        "m_calendar",
        "m_charge",
        "m_client",
        "m_code",
        "m_code_value",
        "m_currency",
        "m_document",
        "m_family_members",
        "m_fund",
        "m_group",
        "m_group_client",
        "m_holiday",
        "m_loan",
        "m_loan_charge",
        "m_loan_collateral",
        "m_loan_repayment_schedule",
        "m_loan_transaction",
        "m_note",
        "m_office",
        "m_payment_detail",
        "m_payment_type",
        "m_product_loan",
        "m_role",
        "m_savings_account",
        "m_savings_account_transaction",
        "m_staff",
    ].map((name) => discoveredTable("public", name, [["Id", "bigint"]])),
};

export const PLANNER_CONVERSATION_ID = "planner/demo_shop/1";

const customersConfig = rootTableConfig({
    collectionName: "Customers",
    sourceTableSchema: "dbo",
    sourceTableName: "Customers",
    columns: [columnMapping("Name", "name"), columnMapping("Email", "email")],
    embeddedTables: [
        embeddedTableConfig({
            sourceTableSchema: null,
            sourceTableName: "CustomerAddresses",
            propertyName: "addresses",
            joinColumns: ["CustomerId"],
            columns: [columnMapping("City", "city"), columnMapping("IsDefault", "isDefault")],
        }),
    ],
});

const orderLinesConfig = embeddedTableConfig({
    sourceTableSchema: null,
    sourceTableName: "OrderLines",
    propertyName: "lines",
    joinColumns: ["OrderId"],
    columns: [columnMapping("ProductId", "productId"), columnMapping("Quantity", "quantity")],
});

const ordersConfig = rootTableConfig({
    collectionName: "Orders",
    sourceTableSchema: "dbo",
    sourceTableName: "Orders",
    columns: [columnMapping("PlacedAt", "placedAt"), columnMapping("Total", "total")],
    linkedTables: [linkedTableConfig("dbo", "Customers", "customer", "CustomerId")],
    embeddedTables: [orderLinesConfig],
});

/** The re-registration that links each order line to its product instead of copying the id. */
const ordersLinkedToProductsConfig = rootTableConfig({
    ...ordersConfig,
    embeddedTables: [
        {
            ...orderLinesConfig,
            columns: [columnMapping("Quantity", "quantity")],
            linkedTables: [linkedTableConfig("dbo", "Products", "product", "ProductId")],
        },
    ],
});

const productsConfig = rootTableConfig({
    collectionName: "Products",
    sourceTableSchema: "dbo",
    sourceTableName: "Products",
    columns: [columnMapping("Name", "name"), columnMapping("Price", "price")],
});

const TOTAL_PRECISION_WARNING =
    "dbo.Orders.Total is decimal(18,2) and is stored as a number, which can lose precision above 2^53.";

/** The planner's first turn: it proposes, explains, and asks before registering anything. */
export const plannerOpeningFrames: MigrationFrame[] = [
    {
        type: "proposal",
        areas: [
            { area: "Customers area", collections: ["Customers"], why: "Who buys, and where they live." },
            { area: "Sales area", collections: ["Orders", "Products"], why: "What is sold, and to whom." },
        ],
        collections: [
            proposedCollection(
                "Customers",
                "dbo.Customers",
                [{ table: "dbo.CustomerAddresses", how: "Embed", why: "Addresses only exist for a customer." }],
                "One document per customer, so a profile loads in a single read.",
            ),
            proposedCollection(
                "Orders",
                "dbo.Orders",
                [
                    { table: "dbo.OrderLines", how: "Embed", why: "Lines only exist for an order." },
                    { table: "dbo.Customers", how: "Link", why: "A customer outlives their orders." },
                ],
                "An order with its lines is the unit every question about sales starts from.",
            ),
            proposedCollection(
                "Products",
                "dbo.Products",
                [],
                "Referenced by many order lines, so it stays a collection of its own.",
            ),
        ],
        dropped: [droppedTable("dbo.AuditLog", "Nothing in the app reads it.")],
        enables: [
            "An agent that answers questions about a customer's orders from one document per order.",
            "Looking up a customer's addresses without a second read.",
            "Reporting on sales by product through order lines that link to it.",
        ],
    },
    {
        type: "reply",
        reply:
            "I read **6 tables** and grouped them into **3 collections**. `dbo.AuditLog` is left out because nothing in the app reads it.\n\n" +
            "Before I register anything, I need a few decisions from you.",
        gaps: ["No column says which address is the customer's default shipping address."],
        openQuestions: [
            {
                question: "How should order lines refer to products?",
                options: ["Link to Products by id", "Copy the product name and price into each line"],
                recommended: 0,
            },
            {
                question: "Which property casing should the documents use?",
                options: ["camelCase", "PascalCase", "snake_case"],
                recommended: 0,
            },
            {
                question: "Should cancelled orders be migrated at all?",
                options: ["No, leave them out", "Yes, migrate them as well"],
                recommended: 0,
            },
        ],
    },
    { type: "done", conversationId: PLANNER_CONVERSATION_ID },
];

/** The answers to the opening questions, as the operator sent them. */
export const plannerAnswers = [
    { question: "How should order lines refer to products?", answer: "Link to Products by id" },
    { question: "Which property casing should the documents use?", answer: "camelCase" },
    { question: "Should cancelled orders be migrated at all?", answer: "No, leave them out" },
];

/** Building what was proposed: two collections pass, one is rejected. */
export const plannerBuildFrames: MigrationFrame[] = [
    { type: "conventions", propertyCase: "CamelCase", propertyLanguage: null, notes: null, mustReEmit: [] },
    {
        type: "collection",
        status: "registered",
        collection: "Customers",
        version: 1,
        rationale: "Addresses are embedded as an array, so a customer's profile is one document.",
        config: customersConfig,
        warnings: [],
    },
    {
        type: "collection",
        status: "registered",
        collection: "Orders",
        version: 1,
        rationale: "Lines are embedded; the customer is linked by id so customer edits never rewrite orders.",
        config: ordersConfig,
        warnings: [TOTAL_PRECISION_WARNING],
    },
    {
        type: "rejected",
        collection: "Products",
        errors: [
            "Column Price is mapped twice, as price and unitPrice.",
            "Primary key column ProductId does not exist on dbo.Products.",
        ],
    },
    {
        type: "reply",
        reply:
            "Registered **Customers** and **Orders** with camelCase properties. **Products** was rejected - the price " +
            "column was mapped twice. Ask me to retry it, or carry on with the two that passed.",
        gaps: [],
        openQuestions: [],
    },
    { type: "done", conversationId: PLANNER_CONVERSATION_ID },
];

export const PLANNER_RETRY_PROMPT = "Retry Products with a single price property.";

/** The retry: Products is registered, and Orders re-registered so its lines link to it. */
export const plannerRetryFrames: MigrationFrame[] = [
    { type: "note", text: "Dropped the duplicate unitPrice mapping" },
    {
        type: "collection",
        status: "registered",
        collection: "Products",
        version: 1,
        rationale: "One document per product, which order lines link to.",
        config: productsConfig,
        warnings: [],
    },
    {
        type: "collection",
        status: "replaced",
        collection: "Orders",
        version: 2,
        rationale: "Order lines now link to Products, so a product rename shows up on every order.",
        config: ordersLinkedToProductsConfig,
        warnings: [TOTAL_PRECISION_WARNING],
    },
    {
        type: "reply",
        reply: "**Products** is registered now, and I re-registered **Orders** so its lines link to it.",
        gaps: [],
        openQuestions: [],
    },
    { type: "done", conversationId: PLANNER_CONVERSATION_ID },
];

/** The configuration as the server assembles it on apply: an embedded table without a schema takes its parent's. */
function asApplied(config: CdcSinkTableConfig): CdcSinkTableConfig {
    const inherit = (
        tables: CdcSinkEmbeddedTableConfig[] | null | undefined,
        parentSchema: string | null | undefined,
    ): CdcSinkEmbeddedTableConfig[] =>
        (tables ?? []).map((table) => {
            const schema = table.sourceTableSchema || parentSchema;

            return { ...table, sourceTableSchema: schema, embeddedTables: inherit(table.embeddedTables, schema) };
        });

    return { ...config, embeddedTables: inherit(config.embeddedTables, config.sourceTableSchema) };
}

export const sampleMigrationApply: MigrationApplyResponse = {
    configuration: { tables: [customersConfig, ordersLinkedToProductsConfig, productsConfig].map(asApplied) },
    unmappedTables: ["dbo.AuditLog"],
    errors: [],
};

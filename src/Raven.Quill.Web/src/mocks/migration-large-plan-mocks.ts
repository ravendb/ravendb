import type {
    CdcSinkTableConfig,
    DiscoverResponse,
    DiscoverTableResponse,
    MigrationFrame,
} from "@/api/generated/server-api";
import { pascalCase } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { discoveredTable } from "@/mocks/discovery-fixtures";
import {
    columnMapping,
    droppedTable,
    embeddedTableConfig,
    proposedCollection,
    rootTableConfig,
} from "@/mocks/migration-mocks";

// A 214-table ERP database for the planner at scale: 200 tables selected, 128 of them in 66 collections,
// 72 left out by the planner and 14 never selected. Names are placeholders.

type CollectionSpec = [name: string, singular: string, fields: string, children: string];

const AREAS: [area: string, schema: string, collections: CollectionSpec[]][] = [
    [
        "Customers area",
        "crm",
        [
            ["Customers", "customer", "name, email", "CustomerAddresses:addresses, CustomerContacts:contacts"],
            ["Accounts", "account", "name, industry", "AccountNotes:notes"],
            ["Leads", "lead", "source, status", ""],
            ["Opportunities", "opportunity", "stage, amount", "OpportunityProducts:products"],
            ["Campaigns", "campaign", "name, startsAt", "CampaignMembers:members"],
            ["Segments", "segment", "name", "SegmentRules:rules"],
            ["LoyaltyCards", "loyalty card", "number, points", "LoyaltyTransactions:transactions"],
        ],
    ],
    [
        "Sales area",
        "sales",
        [
            ["Orders", "order", "placedAt, total", "OrderLines:lines, OrderDiscounts:discounts"],
            ["Quotes", "quote", "validUntil, total", "QuoteLines:lines"],
            ["Returns", "return", "reason, refundTotal", "ReturnLines:lines"],
            ["PriceLists", "price list", "name, currency", "PriceListItems:items"],
            ["Promotions", "promotion", "code, startsAt", "PromotionRules:rules"],
            ["SalesReps", "sales rep", "name, region", "SalesRepQuotas:quotas"],
            ["Territories", "territory", "name, country", ""],
            ["GiftCards", "gift card", "code, balance", "GiftCardRedemptions:redemptions"],
        ],
    ],
    [
        "Catalog area",
        "catalog",
        [
            ["Products", "product", "name, price", "ProductImages:images, ProductAttributes:attributes"],
            ["Categories", "category", "name, slug", ""],
            ["Brands", "brand", "name, country", ""],
            ["Bundles", "bundle", "name, price", "BundleItems:items"],
            ["Reviews", "review", "rating, body", "ReviewVotes:votes"],
            ["UnitsOfMeasure", "unit of measure", "code, factor", ""],
        ],
    ],
    [
        "Inventory area",
        "inventory",
        [
            ["Warehouses", "warehouse", "name, address", "WarehouseZones:zones, Bins:bins"],
            ["StockLevels", "stock level", "onHand, reserved", ""],
            ["StockMovements", "stock movement", "quantity, movedAt", ""],
            ["Transfers", "transfer", "shippedAt, status", "TransferLines:lines"],
            ["StockCounts", "stock count", "countedAt, status", "StockCountLines:lines"],
            ["Lots", "lot", "number, expiresAt", "SerialNumbers:serialNumbers"],
        ],
    ],
    [
        "Purchasing area",
        "purchasing",
        [
            ["Suppliers", "supplier", "name, taxId", "SupplierContacts:contacts, SupplierAddresses:addresses"],
            ["PurchaseOrders", "purchase order", "orderedAt, total", "PurchaseOrderLines:lines"],
            ["Receipts", "receipt", "receivedAt, status", "ReceiptLines:lines"],
            ["SupplierInvoices", "supplier invoice", "number, dueAt", "SupplierInvoiceLines:lines"],
            ["Contracts", "contract", "startsAt, endsAt", "ContractTerms:terms"],
            ["Requisitions", "requisition", "neededBy, status", "RequisitionLines:lines"],
        ],
    ],
    [
        "Fulfillment area",
        "shipping",
        [
            ["Shipments", "shipment", "shippedAt, trackingNumber", "ShipmentItems:items, ShipmentEvents:events"],
            ["Carriers", "carrier", "name, accountNumber", "CarrierServices:services"],
            ["Packages", "package", "weight, size", ""],
            ["PickLists", "pick list", "createdAt, status", "PickListLines:lines"],
            ["DeliveryRoutes", "delivery route", "name, day", "RouteStops:stops"],
            ["ShippingRates", "shipping rate", "zone, price", ""],
        ],
    ],
    [
        "Finance area",
        "finance",
        [
            ["Invoices", "invoice", "number, issuedAt", "InvoiceLines:lines, InvoiceTaxes:taxes"],
            ["Payments", "payment", "paidAt, amount", "PaymentAllocations:allocations"],
            ["CreditNotes", "credit note", "number, issuedAt", "CreditNoteLines:lines"],
            ["JournalEntries", "journal entry", "postedAt, memo", "JournalLines:lines"],
            ["GlAccounts", "GL account", "code, name", ""],
            ["TaxRates", "tax rate", "name, rate", "TaxRegions:regions"],
            ["Currencies", "currency", "code, symbol", "ExchangeRates:rates"],
            ["Budgets", "budget", "year, total", "BudgetLines:lines"],
        ],
    ],
    [
        "Manufacturing area",
        "mfg",
        [
            ["WorkOrders", "work order", "dueAt, quantity", "WorkOrderRoutings:routings, WorkOrderMaterials:materials"],
            ["BillsOfMaterials", "bill of materials", "version, status", "BomLines:lines"],
            ["WorkCenters", "work center", "name, capacity", "WorkCenterShifts:shifts"],
            ["Routings", "routing", "name, version", "RoutingSteps:steps"],
            ["QualityChecks", "quality check", "checkedAt, result", "QualityResults:results"],
            ["Machines", "machine", "name, model", "MachineDowntime:downtime"],
        ],
    ],
    [
        "People area",
        "hr",
        [
            [
                "Employees",
                "employee",
                "name, hiredAt",
                "EmployeeAddresses:addresses, EmergencyContacts:emergencyContacts",
            ],
            ["Departments", "department", "name, costCenter", ""],
            ["Positions", "position", "title, grade", ""],
            ["TimeEntries", "time entry", "date, hours", ""],
            ["LeaveRequests", "leave request", "startsAt, days", "LeaveBalances:balances"],
            ["Payslips", "payslip", "period, net", "PayslipLines:lines"],
            ["Trainings", "training", "name, date", "TrainingAttendees:attendees"],
        ],
    ],
    [
        "Support area",
        "support",
        [
            ["Tickets", "ticket", "subject, status", "TicketMessages:messages, TicketAttachments:attachments"],
            ["Agents", "agent", "name, team", "AgentSkills:skills"],
            ["SlaPolicies", "SLA policy", "name, priority", "SlaTargets:targets"],
            ["KnowledgeArticles", "knowledge article", "title, publishedAt", "ArticleTags:tags"],
            ["Surveys", "survey", "name, sentAt", "SurveyResponses:responses"],
            ["Macros", "macro", "name, body", ""],
        ],
    ],
];

const LEFT_OUT: [tables: string, why: string][] = [
    [
        "audit.CustomersHistory audit.AccountsHistory audit.OrdersHistory audit.QuotesHistory audit.ReturnsHistory audit.PriceListsHistory audit.ProductsHistory audit.CategoriesHistory audit.WarehousesHistory audit.StockLevelsHistory audit.SuppliersHistory audit.PurchaseOrdersHistory audit.ShipmentsHistory audit.InvoicesHistory audit.PaymentsHistory audit.JournalEntriesHistory audit.WorkOrdersHistory audit.EmployeesHistory audit.PayslipsHistory audit.TicketsHistory",
        "History table filled by triggers; no foreign keys point to it.",
    ],
    [
        "staging.CustomerImport staging.OrderImport staging.ProductImport staging.PriceImport staging.StockImport staging.SupplierImport staging.InvoiceImport staging.EmployeeImport staging.ExchangeRateImport staging.TrackingImport",
        "Import staging table; nothing references it.",
    ],
    [
        "sales.Orders_Backup2019 sales.OrderLines_Backup2019 catalog.Products_Old catalog.Categories_Old crm.Customers_Copy finance.Invoices_2018 finance.InvoiceLines_2018 hr.Employees_Bak inventory.StockLevels_Snapshot purchasing.Suppliers_Old shipping.Carriers_Old support.Tickets_Archive",
        "Copy of another table with the same columns.",
    ],
    [
        "log.ApiRequests log.Errors log.Jobs log.Logins log.EmailsSent log.Exports log.Imports log.Webhooks",
        "Log table; no foreign keys point to it.",
    ],
    [
        "dbo.__MigrationHistory dbo.sysdiagrams dbo.__RefactorLog dbo.SchedulerJobs dbo.SchedulerState dbo.DistributedLocks",
        "Tooling table, not app data.",
    ],
    [
        "report.DailySales report.MonthlySales report.StockValuation report.CustomerLifetimeValue report.AgedReceivables report.TopProducts report.SalesByRegion report.PayrollSummary",
        "Totals computed from other tables. A map-reduce index can replace it.",
    ],
    [
        "sales.LegacyDiscounts crm.LeadScores_v1 catalog.Keywords inventory.TempReservations finance.TempAllocations hr.LegacyBadges purchasing.OldApprovals shipping.ManualLabels",
        "No foreign keys in or out, and no view or procedure uses it.",
    ],
];

const NOT_SELECTED =
    "archive.Orders2015 archive.Orders2016 archive.Orders2017 archive.Invoices2015 archive.Invoices2016 test.SeedCustomers test.SeedProducts catalog.ProductVariants crm.CustomerPreferences sales.OrderNotes finance.PaymentMethods inventory.StockReservations hr.EmployeeSkills support.TicketTags";

const FAILURES: Record<string, string[]> = {
    Invoices: [
        "Column TaxAmount is mapped twice, as taxAmount and tax.",
        "Join column InvoiceId does not exist on finance.InvoiceTaxes.",
    ],
    Shipments: ["Primary key column ShipmentEventId does not exist on shipping.ShipmentEvents."],
    WorkOrders: ["Embedded table mfg.WorkOrderMaterials has no primary key."],
};

const UPDATED = ["Customers", "PurchaseOrders", "Payslips"];

export const LARGE_PLAN_CONVERSATION_ID = "planner/acme_erp/1";

type GeneratedCollection = {
    area: string;
    schema: string;
    name: string;
    singular: string;
    fields: string[];
    children: { table: string; property: string }[];
};

const collections: GeneratedCollection[] = AREAS.flatMap(([area, schema, specs]) =>
    specs.map(([name, singular, fields, children]) => ({
        area,
        schema,
        name,
        singular,
        fields: fields.split(", "),
        children: children
            ? children.split(", ").map((child) => {
                  const [table, property] = child.split(":");
                  return { table, property };
              })
            : [],
    })),
);

const leftOutTables = LEFT_OUT.flatMap(([tables, why]) => tables.split(" ").map((table) => ({ table, why })));

function foreignKeyOf(collection: GeneratedCollection): string {
    return `${pascalCase(collection.singular)}Id`;
}

function table(label: string, columns: string[], references: Record<string, string> = {}): DiscoverTableResponse {
    const [schema, name] = label.split(".");
    const typedColumns = ["Id", ...columns].map((column): [string, string] => [
        column,
        column.endsWith("Id") ? "int" : "nvarchar(200)",
    ]);

    return discoveredTable(schema, name, typedColumns, references);
}

export const largePlanDiscovery: DiscoverResponse = {
    catalogName: "acme_erp",
    success: true,
    hasPermissionToSetup: true,
    errors: [],
    warnings: [],
    tables: [
        ...collections.flatMap((collection) => [
            table(`${collection.schema}.${collection.name}`, collection.fields.map(pascalCase)),
            ...collection.children.map((child) =>
                table(`${collection.schema}.${child.table}`, [foreignKeyOf(collection), "Details"], {
                    [foreignKeyOf(collection)]: collection.name,
                }),
            ),
        ]),
        ...leftOutTables.map(({ table: label }) => table(label, ["Entry"])),
        ...NOT_SELECTED.split(" ").map((label) => table(label, ["Entry"])),
    ],
};

export const largePlanSelectedTables = largePlanDiscovery.tables
    .filter(
        (discovered) =>
            !NOT_SELECTED.split(" ").includes(`${discovered.sourceTableSchema}.${discovered.sourceTableName}`),
    )
    .map((discovered) => ({
        sourceTableSchema: discovered.sourceTableSchema ?? null,
        sourceTableName: discovered.sourceTableName,
    }));

function configOf(collection: GeneratedCollection): CdcSinkTableConfig {
    return rootTableConfig({
        collectionName: collection.name,
        sourceTableSchema: collection.schema,
        sourceTableName: collection.name,
        columns: collection.fields.map((field) => columnMapping(pascalCase(field), field)),
        embeddedTables: collection.children.map((child) =>
            embeddedTableConfig({
                sourceTableSchema: collection.schema,
                sourceTableName: child.table,
                propertyName: child.property,
                joinColumns: [foreignKeyOf(collection)],
                columns: [columnMapping("Details", "details")],
            }),
        ),
    });
}

export const largePlanOpeningFrames: MigrationFrame[] = [
    {
        type: "proposal",
        areas: AREAS.map(([area, , specs]) => ({ area, collections: specs.map(([name]) => name), why: null })),
        collections: collections.map((collection) =>
            proposedCollection(
                collection.name,
                `${collection.schema}.${collection.name}`,
                collection.children.map((child) => ({
                    table: `${collection.schema}.${child.table}`,
                    how: "Embed" as const,
                })),
                `One document per ${collection.singular}.`,
            ),
        ),
        dropped: leftOutTables.map(({ table: label, why }) => droppedTable(label, why)),
        enables: [],
    },
    {
        type: "reply",
        reply:
            `I read **200 tables** and grouped 128 of them into **${collections.length} collections**. The other ` +
            `${leftOutTables.length} look like history, staging, log, report or backup tables, or nothing uses them. ` +
            "They are listed in the plan under Tables.",
        gaps: [],
        openQuestions: [],
    },
    { type: "done", conversationId: LARGE_PLAN_CONVERSATION_ID },
];

export const largePlanBuildFrames: MigrationFrame[] = [
    ...collections.map((collection): MigrationFrame => {
        const errors = FAILURES[collection.name];

        return errors
            ? {
                  type: "rejectedCollection",
                  collection: collection.name,
                  config: configOf(collection),
                  errors: errors.map((message) => ({ message })),
              }
            : {
                  type: "collection",
                  status: "registered",
                  collection: collection.name,
                  version: UPDATED.includes(collection.name) ? 2 : 1,
                  rationale: `One document per ${collection.singular}.`,
                  config: configOf(collection),
                  warnings: [],
              };
    }),
    {
        type: "reply",
        reply:
            `Registered ${collections.length - Object.keys(FAILURES).length} collections. **Invoices**, **Shipments** ` +
            "and **WorkOrders** were rejected. Ask me to retry them, or carry on without them.",
        gaps: [],
        openQuestions: [],
    },
    { type: "done", conversationId: LARGE_PLAN_CONVERSATION_ID },
];

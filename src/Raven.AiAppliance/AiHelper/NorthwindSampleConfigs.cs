using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.AiAppliance.AiHelper;

/// <summary>
/// Hard-coded Northwind sample configs served by <see cref="MockAiHelperClient"/> while the
/// internal AI service is unavailable. The CDC config mirrors the dataset used by the E2E
/// <c>ApplianceFullFlowTests</c> (the canonical <c>northwind-full</c> dump: lowercase plural
/// tables, snake_case columns), so the front end sees realistic, internally consistent data
/// across the wizard (CDC mapping) and the agent Review form.
/// </summary>
internal static class NorthwindSampleConfigs
{
    /// <summary>
    /// One Northwind <see cref="CdcSinkConfiguration"/> covering Customers, Orders, and Products.
    /// <see cref="CdcSinkConfiguration.ConnectionStringName"/> is set because the wizard's suggest
    /// endpoint re-runs <see cref="CdcSinkConfiguration.Validate"/>, which rejects an empty
    /// connection-string name even with <c>validateConnection: false</c>. Every primary-key column
    /// appears in the column mappings, satisfying the second Validate requirement.
    /// </summary>
    public static CdcSinkConfiguration BuildCdcConfig() => new()
    {
        Name = "wizard-cdc",
        ConnectionStringName = "northwind-source",
        Tables =
        [
            new CdcSinkTableConfig
            {
                CollectionName = "Customers",
                SourceTableSchema = "public",
                SourceTableName = "customers",
                PrimaryKeyColumns = ["customer_id"],
                Columns =
                [
                    new CdcColumnMapping { Column = "customer_id",  Name = "Id" },
                    new CdcColumnMapping { Column = "company_name", Name = "CompanyName" },
                    new CdcColumnMapping { Column = "contact_name", Name = "ContactName" },
                    new CdcColumnMapping { Column = "city",         Name = "City" },
                    new CdcColumnMapping { Column = "country",      Name = "Country" },
                ],
            },
            new CdcSinkTableConfig
            {
                CollectionName = "Orders",
                SourceTableSchema = "public",
                SourceTableName = "orders",
                PrimaryKeyColumns = ["order_id"],
                Columns =
                [
                    new CdcColumnMapping { Column = "order_id",    Name = "Id" },
                    new CdcColumnMapping { Column = "customer_id", Name = "CustomerId" },
                    new CdcColumnMapping { Column = "order_date",  Name = "OrderDate" },
                    new CdcColumnMapping { Column = "freight",     Name = "Freight" },
                ],
            },
            new CdcSinkTableConfig
            {
                CollectionName = "Products",
                SourceTableSchema = "public",
                SourceTableName = "products",
                PrimaryKeyColumns = ["product_id"],
                Columns =
                [
                    new CdcColumnMapping { Column = "product_id",     Name = "Id" },
                    new CdcColumnMapping { Column = "product_name",   Name = "ProductName" },
                    new CdcColumnMapping { Column = "unit_price",     Name = "UnitPrice" },
                    new CdcColumnMapping { Column = "units_in_stock", Name = "UnitsInStock" },
                    new CdcColumnMapping { Column = "discontinued",   Name = "Discontinued" },
                ],
            },
        ],
    };

    /// <summary>
    /// Three distinct agent candidates derived from the mirrored Northwind collections, representing
    /// the <c>from-data</c> mode result. <see cref="AiAgentConfiguration.ConnectionStringName"/> is
    /// left empty: the operator picks an existing AI connection string in the Review form, and the
    /// suggest endpoint does not validate it (only provisioning does).
    /// </summary>
    public static AiAgentConfiguration[] BuildDataModeAgents() =>
    [
        new AiAgentConfiguration
        {
            Identifier = "order-support",
            Name = "Order Support Assistant",
            SystemPrompt =
                "You are a customer-support assistant for the Northwind store. Help shoppers find " +
                "their orders, check order dates and freight charges, and answer questions about a " +
                "customer's purchase history. Use the provided query tools and never invent order data.",
            Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "findOrdersByCustomer",
                    Description = "Returns the orders placed by a given customer, most recent first.",
                    Query = "from Orders where CustomerId = $customerId order by OrderDate desc",
                    ParametersSampleObject = "{ \"customerId\": \"customers/ALFKI\" }",
                },
            ],
            Parameters =
            [
                new AiAgentParameter { Name = "customerId", Description = "The id of the customer whose orders to look up." },
            ],
        },
        new AiAgentConfiguration
        {
            Identifier = "product-catalog",
            Name = "Product Catalog Assistant",
            SystemPrompt =
                "You are a product-catalog assistant for the Northwind store. Help shoppers search " +
                "the catalog, compare prices, and check stock availability. Mention when a product is " +
                "discontinued. Only answer from the catalog data returned by the query tools.",
            Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "searchProducts",
                    Description = "Searches products by name and returns price and stock information.",
                    Query = "from Products where search(ProductName, $term) and Discontinued = false",
                    ParametersSampleObject = "{ \"term\": \"tea*\" }",
                },
            ],
            Parameters =
            [
                new AiAgentParameter { Name = "term", Description = "A product-name search term (supports the * wildcard)." },
            ],
        },
        new AiAgentConfiguration
        {
            Identifier = "sales-insights",
            Name = "Sales Insights Analyst",
            SystemPrompt =
                "You are a sales-insights analyst for the Northwind store. Answer questions about " +
                "order volume, freight spend, and customer activity over a date range. Summarize the " +
                "numbers returned by the query tools; do not fabricate figures.",
            Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "ordersInRange",
                    Description = "Returns orders placed between two dates for aggregate analysis.",
                    Query = "from Orders where OrderDate between $from and $to",
                    ParametersSampleObject = "{ \"from\": \"1997-01-01\", \"to\": \"1997-12-31\" }",
                },
            ],
            Parameters =
            [
                new AiAgentParameter { Name = "from", Description = "Inclusive start date (ISO-8601)." },
                new AiAgentParameter { Name = "to", Description = "Inclusive end date (ISO-8601)." },
            ],
        },
    ];

    /// <summary>
    /// Single agent candidate for the <c>from-prompt</c> mode. The operator's intent prompt is
    /// folded into the system prompt, letting the front end demonstrate that prompt-mode reflects
    /// the supplied input.
    /// </summary>
    public static AiAgentConfiguration BuildPromptModeAgent(string? prompt)
    {
        var intent = string.IsNullOrWhiteSpace(prompt)
            ? "Help shoppers with their questions about the Northwind store."
            : prompt.Trim();

        return new AiAgentConfiguration
        {
            Identifier = "northwind-assistant",
            Name = "Northwind Assistant",
            SystemPrompt =
                "You are an assistant for the Northwind store, backed by the Customers, Orders, and " +
                "Products collections. " + intent + " Answer only from the data returned by the query " +
                "tools and ask for clarification when a request is ambiguous.",
            Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "findOrdersByCustomer",
                    Description = "Returns the orders placed by a given customer, most recent first.",
                    Query = "from Orders where CustomerId = $customerId order by OrderDate desc",
                    ParametersSampleObject = "{ \"customerId\": \"customers/ALFKI\" }",
                },
            ],
            Parameters =
            [
                new AiAgentParameter { Name = "customerId", Description = "The id of the customer whose orders to look up." },
            ],
        };
    }
}

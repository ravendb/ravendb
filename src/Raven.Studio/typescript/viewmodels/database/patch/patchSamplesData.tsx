import React from "react";
import { MethodGroup, SampleScript } from "components/common/sampleQueries/partials/sampleQueriesTypes";

export const scripts: SampleScript[] = [
    {
        title: "Filter out an array item",
        description: "",
        script: `// Removes a specific line item (product ID 'products/1-A') from all order documents.
from Orders
update {
    this.Lines = this.Lines.filter(l => l.Product != 'products/1-A');
}`,
    },
    {
        title: "Denormalize the company name",
        description: "",
        script: `// Adds a CompanyName field to all orders, populated from a related document.
from Orders as o
load o.Company as c
update {
    o.CompanyName = c.Name;
}`,
    },
    {
        title: "Use JavaScript to patch",
        description: "",
        script: `// Conditionally adds a new lowercase company name field to orders over 10,000.
from index 'Orders/Totals' as i
where i.Total > 10000
load i.Company as c
update {
    i.LowerName = c.Name.toLowerCase();
}`,
    },
    {
        title: "Access the metadata",
        description: "",
        script: `// Adds the document ID and collection name as two new fields to every order.
from Orders
update {
    this.DocumentId = id(this);
    this.DocumentCollection = this["@metadata"]["@collection"];
}`,
    },
    {
        title: "Add a time series entry",
        description: "",
        script: `// Appends a time series entry to a specific employee document.
from Employees as e
where id() = 'employees/1-A'
update {
    timeseries(e, "HeartRates")
        .append("2020-06-25T10:48:14.794", [120, 80], "TagName");
}`,
    },
    {
        title: "Update a field on a whole collection",
        description: "",
        script: `// Increases the Freight value by 10 on every order.
from Orders as o
update {
    o.Freight += 10;
}`,
    },
    {
        title: "Rename a field",
        description: "",
        script: `// Copies Freight into a new ShippingCost field, then removes the old field.
from Orders as o
update {
    o.ShippingCost = o.Freight;
    delete o.Freight;
}`,
    },
    {
        title: "Update each item in an array",
        description: "",
        script: `// Applies a 10% discount to every line item in all orders.
from Orders as o
update {
    o.Lines.forEach(line => line.Discount = 0.1);
}`,
    },
    {
        title: "Increment a counter",
        description: "",
        script: `// Increments a 'Views' counter by 1 on every product.
from Products as p
update {
    incrementCounter(p, "Views", 1);
}`,
    },
    {
        title: "Create a related document",
        description: "",
        script: `// Writes a new audit document for each order, using a server-generated ID.
from Orders as o
update {
    put("auditLogs/", { OrderId: id(o), PatchedAt: new Date().toISOString() });
}`,
    },
    {
        title: "Patch documents by ID",
        description: "",
        script: `// Sets a field on a specific set of documents selected by their IDs.
from @all_docs as d
where id() in ('orders/1-A', 'companies/1-A')
update {
    d.Updated = true;
}`,
    },
    {
        title: "Delete documents by query",
        description: "",
        script: `// Deletes all orders with freight over 200.
from Orders as o
where o.Freight > 200
update {
    del(id(o));
}`,
    },
    {
        title: "Change the collection",
        description: "",
        script: `// Moves matching orders into a new collection by deleting and re-creating each document.
from Orders as o
update {
    o["@metadata"]["@collection"] = "Archived_Orders";
    del(id(o));
    put(id(o), o);
}`,
    },
];

export const methodGroups: MethodGroup[] = [
    {
        category: "Document operations",
        methods: [
            {
                signature: "id(document)",
                returnType: "string",
                description: (
                    <>
                        Returns the ID of a document object. Use <code>id(this)</code> for the document currently being
                        patched. Use <code>id(documentVariable)</code> for another document object available in the
                        script, such as a loaded related document.
                    </>
                ),
                sampleScript: `// Copy the current order ID and the related company ID into fields.
from Orders as o
load o.Company as c
update {
    o.OrderId = id(this); // Current order ID
    o.CompanyId = id(c);  // Loaded company ID
}`,
            },
            {
                signature: "getMetadata(document)",
                returnType: "object",
                description: (
                    <>
                        Returns the document&apos;s metadata, e.g. <code>@id</code>, <code>@collection</code>,{" "}
                        <code>@last-modified</code>. Also available as <code>metadataFor(document)</code>.
                    </>
                ),
                sampleScript: `// Read the collection name from metadata into a field.
from Orders as o
update {
    o.Collection = getMetadata(o)["@collection"];
}`,
            },
            {
                signature: "lastModified(document)",
                returnType: "number",
                description: (
                    <>
                        Returns the document&apos;s last modification time as JavaScript milliseconds since the Unix
                        epoch (UTC).
                    </>
                ),
                sampleScript: `// Store each order's last-modified time (ms) in a field.
from Orders as o
update {
    o.LastModified = lastModified(o);
}`,
            },
            {
                signature: "load(documentId)",
                returnType: "object | object[] | undefined",
                description: <>Returns document(s) with the given ID, or undefined if not found.</>,
                sampleScript: `// Load the related company and copy its name.
from Orders as o
update {
    o.CompanyName = load(o.Company).Name;
}`,
            },
            {
                signature: "loadPath(document, pathString)",
                returnType: "object | object[] | null",
                description: (
                    <>
                        Returns the document(s) referenced by the IDs found at <code>pathString</code> within the given
                        document (e.g. <code>Foo.Bar</code> or <code>Foo.Bars[].Buzz</code>), or null if not found.
                    </>
                ),
                sampleScript: `// Load all product documents referenced by the order lines.
from Orders as o
update {
    o.Products = loadPath(o, "Lines[].Product");
}`,
            },
            {
                signature: "put(id, document[, changeVector])",
                returnType: "string",
                description: (
                    <>
                        Creates or updates a document and returns its ID. Use <code>null</code> or <code>undefined</code>{" "}
                        to generate an ID.
                    </>
                ),
                sampleScript: `// Create a new audit-log document for each order.
from Orders as o
update {
    put("auditLogs/", { OrderId: id(o), PatchedAt: new Date().toISOString() });
}`,
            },
            {
                signature: "del(documentId[, changeVector])",
                returnType: "boolean",
                description: (
                    <>
                        Deletes the document with the specified ID and returns <code>true</code> if it was deleted;
                        otherwise, returns <code>false</code>.
                    </>
                ),
                sampleScript: `// Delete orders with freight over 200.
from Orders as o
where o.Freight > 200
update {
    del(id(o));
}`,
            },
            {
                signature: "archived.archiveAt(document, utcDateString)",
                returnType: "void",
                description: <>Schedules the document to be archived at the specified UTC time.</>,
                sampleScript: `// Schedule each order to be archived at a future date.
from Orders as o
update {
    archived.archiveAt(o, "2030-01-01T00:00:00Z");
}`,
            },
            {
                signature: "archived.unarchive(document)",
                returnType: "void",
                description: <>Unarchives the specified document.</>,
                sampleScript: `// Unarchive all orders.
from Orders as o
update {
    archived.unarchive(o);
}`,
            },
        ],
    },
    {
        category: "Counter operations",
        methods: [
            {
                signature: "counter(document/documentId, name)",
                returnType: "number | null",
                description: <>Get the counter value, or null if the counter does not exist.</>,
                sampleScript: `// Read the "Views" counter into a field.
from Products as p
update {
    p.Views = counter(p, "Views");
}`,
            },
            {
                signature: "counterRaw(document/documentId, name)",
                returnType: "object",
                description: (
                    <>
                        Get the per-node counter values by document/document ID. The total value is the sum of all node
                        values.
                    </>
                ),
                sampleScript: `// Read the per-node "Views" counter values into a field.
from Products as p
update {
    p.ViewsPerNode = counterRaw(p, "Views");
}`,
            },
            {
                signature: "incrementCounter(document/documentId, name, value = 1)",
                returnType: "boolean",
                description: (
                    <>
                        Increments a counter by document/document ID and returns <code>true</code>. Created implicitly
                        if it does not exist; values may be negative.
                    </>
                ),
                sampleScript: `// Increment the "Views" counter by 1 on every product.
from Products as p
update {
    incrementCounter(p, "Views", 1);
}`,
            },
            {
                signature: "deleteCounter(document/documentId, name)",
                returnType: "boolean",
                description: (
                    <>
                        Deletes a counter by document/document ID and returns <code>true</code>.
                    </>
                ),
                sampleScript: `// Delete the "Views" counter from every product.
from Products as p
update {
    deleteCounter(p, "Views");
}`,
            },
        ],
    },
    {
        category: "Time series",
        methods: [
            {
                signature: "timeseries(document/documentId, name).get() / .get(from, to)",
                returnType: "object[]",
                description: <>Get all entries, or a range of time series entries.</>,
                sampleScript: `// Read all "HeartRates" time series entries into a field.
from Employees as e
update {
    e.HeartRates = timeseries(e, "HeartRates").get();
}`,
            },
            {
                signature: "timeseries(document/documentId, name).append(timestamp, values, tag = null)",
                returnType: "void",
                description: <>Append a new entry to a time series.</>,
                sampleScript: `// Append a new entry to a specific employee's "HeartRates" time series.
from Employees as e
where id() = 'employees/1-A'
update {
    timeseries(e, "HeartRates").append("2024-01-01T08:00:00Z", [78], "watch");
}`,
            },
            {
                signature: "timeseries(document/documentId, name).increment([timestamp,] values)",
                returnType: "void",
                description: (
                    <>
                        Increment an incremental time series entry. Omit <code>timestamp</code> to use the current time.
                    </>
                ),
                sampleScript: `// Increment today's value in the "DailyViews" time series.
from Products as p
update {
    timeseries(p, "DailyViews").increment([1]);
}`,
            },
            {
                signature: "timeseries(document/documentId, name).delete() / .delete(from, to)",
                returnType: "void",
                description: <>Delete all entries, or delete entries from the specified time range.</>,
                sampleScript: `// Delete all "HeartRates" time series entries.
from Employees as e
update {
    timeseries(e, "HeartRates").delete();
}`,
            },
            {
                signature: "timeseries(document/documentId, name).getStats()",
                returnType: "object",
                description: (
                    <>
                        Returns the time series statistics: <code>Start</code>, <code>End</code>, and <code>Count</code>.
                    </>
                ),
                sampleScript: `// Store "HeartRates" time series statistics in a field.
from Employees as e
update {
    e.HeartRateStats = timeseries(e, "HeartRates").getStats();
}`,
            },
        ],
    },
    {
        category: "Attachment operations",
        methods: [
            {
                signature: "attachments(document/documentId, name).delete()",
                returnType: "void",
                description: <>Deletes the specified attachment from the document.</>,
                sampleScript: `// Delete the "image.jpg" attachment from each category.
from Categories as c
update {
    attachments(c, "image.jpg").delete();
}`,
            },
            {
                signature:
                    "attachments(targetDocument/targetDocumentId, targetName).copyFrom(sourceDocument/sourceDocumentId, sourceName)",
                returnType: "boolean",
                description: (
                    <>
                        Copies an attachment from a source document to the target document. Returns <code>false</code> if
                        the source attachment was not found.
                    </>
                ),
                sampleScript: `// Copy an attachment from one category to another.
from Categories as c
where id() = 'categories/2-A'
update {
    attachments(c, "copied-image.jpg").copyFrom('categories/1-A', "image.jpg");
}`,
            },
            {
                signature: "attachments(document/documentId, name).remote(identifier, at)",
                returnType: "void",
                description: <>Schedules the attachment for upload to remote storage at the specified UTC time.</>,
                sampleScript: `// Schedule an attachment for upload to remote storage.
from Categories as c
update {
    attachments(c, "image.jpg").remote("s3-archive", "2030-01-01T00:00:00Z");
}`,
            },
        ],
    },
    {
        category: "Compare-exchange",
        methods: [
            {
                signature: "cmpxchg(compareExchangeKey)",
                returnType: "any",
                description: (
                    <>
                        Returns the value stored in the compare-exchange item for the given key, or <code>null</code> if
                        it does not exist.
                    </>
                ),
                sampleScript: `// Look up the USD rate from a compare-exchange value.
from Orders as o
update {
    o.UsdRate = cmpxchg("rates/USD");
}`,
            },
        ],
    },
    {
        category: "String manipulation",
        methods: [
            {
                signature: "startsWith(inputString, prefix)",
                returnType: "boolean",
                description: (
                    <>
                        Returns <code>true</code> if <code>inputString</code> starts with the specified prefix. The
                        comparison is case-insensitive.
                    </>
                ),
                sampleScript: `// Flag companies whose name starts with "Alfreds".
from Companies as c
update {
    c.IsAlfreds = startsWith(c.Name, "Alfreds");
}`,
            },
            {
                signature: "endsWith(inputString, suffix)",
                returnType: "boolean",
                description: (
                    <>
                        Returns <code>true</code> if <code>inputString</code> ends with the specified suffix. The
                        comparison is case-insensitive.
                    </>
                ),
                sampleScript: `// Flag companies whose name ends with "Ltda".
from Companies as c
update {
    c.IsLtda = endsWith(c.Name, "Ltda");
}`,
            },
            {
                signature: "regex(inputString, regex)",
                returnType: "boolean",
                description: (
                    <>
                        Returns <code>true</code> if <code>inputString</code> matches the specified regex pattern.
                    </>
                ),
                sampleScript: `// Flag companies whose name contains a digit.
from Companies as c
update {
    c.NameHasDigit = regex(c.Name, "[0-9]");
}`,
            },
            {
                signature: "String.prototype.format(arg1, arg2, ...)",
                returnType: "string",
                description: (
                    <>
                        Formats the string, replacing each <code>{"{index}"}</code> with the corresponding zero-based
                        argument.
                    </>
                ),
                sampleScript: `// Build a label string from the order ID and company.
from Orders as o
update {
    o.Label = "Order {0} for {1}".format(id(o), o.Company);
}`,
            },
        ],
    },
    {
        category: "Arrays & objects",
        methods: [
            {
                signature: "Object.map(input, mapFunction, context)",
                returnType: "any[]",
                description: (
                    <>
                        Returns an array of <code>mapFunction(value, key)</code> applied to every property (or item) of{" "}
                        <code>input</code>.
                    </>
                ),
                sampleScript: `// Compute a line-total for each order line.
from Orders as o
update {
    o.LineTotals = Object.map(o.Lines, line => line.PricePerUnit * line.Quantity);
}`,
            },
        ],
    },
    {
        category: "Mathematical operations",
        methods: [
            {
                signature: "Raven_Min(value1, value2)",
                returnType: "number | string | boolean",
                description: (
                    <>Returns the smaller value. Supports numbers, strings, booleans, and null/undefined values.</>
                ),
                sampleScript: `// Cap each order's freight at 100.
from Orders as o
update {
    o.CappedFreight = Raven_Min(o.Freight, 100);
}`,
            },
            {
                signature: "Raven_Max(value1, value2)",
                returnType: "number | string | boolean",
                description: (
                    <>Returns the larger value. Supports numbers, strings, booleans, and null/undefined values.</>
                ),
                sampleScript: `// Apply a minimum charged freight of 10.
from Orders as o
update {
    o.MinChargedFreight = Raven_Max(o.Freight, 10);
}`,
            },
        ],
    },
    {
        category: "Spatial",
        methods: [
            {
                signature: 'spatial.distance(lat1, lng1, lat2, lng2, units = "kilometers")',
                returnType: "number",
                description: (
                    <>
                        Returns the distance between two points. <code>units</code> may be{" "}
                        <code>&apos;kilometers&apos;</code> (default), <code>&apos;miles&apos;</code>, or{" "}
                        <code>&apos;cartesian&apos;</code>.
                    </>
                ),
                sampleScript: `// Compute each company's distance to London (km).
from Companies as c
update {
    c.DistanceToLondon = spatial.distance(c.Address.Location.Latitude, c.Address.Location.Longitude, 51.5, -0.12);
}`,
            },
        ],
    },
    {
        category: "Conversion & dates",
        methods: [
            {
                signature: "scalarToRawString(document, lambdaToField)",
                returnType: "string | number | boolean | null",
                description: (
                    <>
                        Returns the field value while preserving raw string and numeric values. Useful for numbers
                        exceeding <code>double</code> range or large strings.
                    </>
                ),
                sampleScript: `// Read the raw, immutable Freight value.
from Orders as o
update {
    o.RawFreight = scalarToRawString(o, x => x.Freight);
}`,
            },
            {
                signature: "convertJsTimeToTimeSpanString(milliseconds)",
                returnType: "string",
                description: (
                    <>
                        Returns the .NET <code>TimeSpan</code> string for the given JavaScript milliseconds value.
                    </>
                ),
                sampleScript: `// Compute how long ago each order was modified.
from Orders as o
update {
    o.Age = convertJsTimeToTimeSpanString(Date.now() - lastModified(o));
}`,
            },
            {
                signature:
                    "convertToTimeSpanString(ticks | hours, minutes, seconds | days, hours, minutes, seconds[, milliseconds])",
                returnType: "string",
                description: <>Returns a <code>TimeSpan</code> built from the supplied time components.</>,
                sampleScript: `// Store a fixed 1h30m processing time as a TimeSpan.
from Orders as o
update {
    o.ProcessingTime = convertToTimeSpanString(1, 30, 0);
}`,
            },
            {
                signature: 'compareDates(date1, date2, operationType = "Subtract")',
                returnType: "string | boolean",
                description: (
                    <>
                        Subtracts or compares two dates. <code>operationType</code> is an <code>ExpressionType</code>{" "}
                        such as <code>GreaterThan</code> or <code>Equal</code>.
                    </>
                ),
                sampleScript: `// Compute the time between order and shipping dates.
from Orders as o
where o.ShippedAt != null
update {
    o.TimeToShip = compareDates(o.ShippedAt, o.OrderedAt);
}`,
            },
            {
                signature: "toStringWithFormat(object, format?, culture?)",
                returnType: "string",
                description: (
                    <>
                        Formats a date, number, boolean, or date-string using an optional .NET format and culture.
                    </>
                ),
                sampleScript: `// Format Freight as a currency string.
from Orders as o
update {
    o.FreightText = toStringWithFormat(o.Freight, "C", "en-US");
}`,
            },
        ],
    },
    {
        category: "Cryptographic methods",
        methods: [
            {
                signature: "crypto.randomUUID()",
                returnType: "string",
                description: <>Generates a random v4 UUID.</>,
                sampleScript: `// Assign a random trace ID to each order.
from Orders as o
update {
    o.TraceId = crypto.randomUUID();
}`,
            },
            {
                signature: "crypto.getRandomValues(typedArray)",
                returnType: "TypedArray",
                description: (
                    <>
                        Fills the provided typed array (<code>ArrayBufferView</code>) with cryptographically secure
                        random values.
                    </>
                ),
                sampleScript: `// Generate a random 16-byte nonce.
from Orders as o
update {
    var nonce = crypto.getRandomValues(new Uint8Array(16));
    o.Nonce = Array.from(nonce);
}`,
            },
            {
                signature: "crypto.getRandomValuesBase64(lenInBytes)",
                returnType: "string",
                description: (
                    <>
                        Generates <code>lenInBytes</code> cryptographically secure random bytes and returns them
                        Base64-encoded.
                    </>
                ),
                sampleScript: `// Generate a random Base64 token.
from Orders as o
update {
    o.Token = crypto.getRandomValuesBase64(16);
}`,
            },
            {
                signature: "crypto.digest(algorithm, data)",
                returnType: "string",
                description: (
                    <>
                        Computes a Base64-encoded hash of <code>data</code> using <code>SHA-256</code>,{" "}
                        <code>SHA-384</code>, or <code>SHA-512</code>.
                    </>
                ),
                sampleScript: `// Compute a SHA-256 hash of the order ID.
from Orders as o
update {
    o.Hash = crypto.digest("SHA-256", id(o));
}`,
            },
            {
                signature: "crypto.sign(hash, key, data)",
                returnType: "string",
                description: (
                    <>
                        Computes a Base64-encoded HMAC signature over <code>data</code>.
                    </>
                ),
                sampleScript: `// Compute an HMAC signature over the order ID.
from Orders as o
update {
    o.Signature = crypto.sign("SHA-256", "secret-key", id(o));
}`,
            },
            {
                signature: "crypto.verify(hash, key, signature, data)",
                returnType: "boolean",
                description: (
                    <>
                        Verifies an HMAC signature over <code>data</code>.
                    </>
                ),
                sampleScript: `// Verify an HMAC signature over the order ID.
from Orders as o
update {
    var signature = crypto.sign("SHA-256", "secret-key", id(o));
    o.SignatureValid = crypto.verify("SHA-256", "secret-key", signature, id(o));
}`,
            },
            {
                signature: "crypto.encryptAesGcm(iv, key, data)",
                returnType: "string",
                description: (
                    <>
                        Encrypts <code>data</code> with AES-GCM and returns it Base64-encoded.
                    </>
                ),
                sampleScript: `// Encrypt the company field with AES-GCM.
from Orders as o
update {
    var key = crypto.getRandomValuesBase64(32);
    var iv = crypto.getRandomValuesBase64(12);
    o.EncryptedCompany = crypto.encryptAesGcm(iv, key, o.Company);
}`,
            },
            {
                signature: "crypto.decryptAesGcm(iv, key, data, outputType?)",
                returnType: "string | ArrayBuffer",
                description: (
                    <>
                        Decrypts AES-GCM <code>data</code>. <code>outputType</code>: <code>&apos;string&apos;</code>{" "}
                        (default), <code>&apos;raw&apos;</code>, or <code>&apos;buffer&apos;</code>.
                    </>
                ),
                sampleScript: `// Decrypt an encrypted company field.
from Orders as o
update {
    var key = crypto.getRandomValuesBase64(32);
    var iv = crypto.getRandomValuesBase64(12);
    var encrypted = crypto.encryptAesGcm(iv, key, o.Company);
    o.DecryptedCompany = crypto.decryptAesGcm(iv, key, encrypted, "string");
}`,
            },
        ],
    },
    {
        category: "Debugging",
        methods: [
            {
                signature: "output(message)",
                returnType: "void",
                description: (
                    <>
                        Prints a message to the debug output when testing. Also available as{" "}
                        <code>console.log(message)</code>.
                    </>
                ),
                sampleScript: `// Print a debug message for each order being patched.
from Orders as o
update {
    output("Patching order " + id(o));
}`,
            },
        ],
    },
];

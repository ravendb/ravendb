# Glossary

One concept, one word, on every surface. These are the product's terms — use them in
every label, heading, menu item, description, toast, `aria-label` and page title.

| Term                  | Means                                        | Never                              |
| --------------------- | -------------------------------------------- | ---------------------------------- |
| **App**               | a configured application                     | "Application"                      |
| **Data source**       | the source database and its ingestion        | "CDC" as a product noun            |
| **Dashboard API key** | the single dashboard credential              | "operator API key", "operator key" |
| **Prompt**            | one message a person sent to an agent        | "Message" for the counted metric   |
| **Quill**             | the deployment                               | "appliance", "this Quill instance" |
| **Sync**              | the running pipeline (performance, progress) | "CDC performance", "CDC feed"      |

**Dashboard API key** is the term when naming the dashboard credential in prose. Bare
"API key" is fine in a field-level message sitting beneath a correctly labelled
"Dashboard API key" field, and it is the correct term for a third-party provider's key
(OpenAI, Azure, Google, Hugging Face, Mistral) — those are never the dashboard credential.

New and changed labels should use sentence case ("IP configuration", "Data source").
**Quill** is a proper noun and stays capitalised.

Only user-visible text is governed here. Identifiers, filenames, API paths and code comments
keep the technical names of the mechanisms they describe — `CdcSinkConfiguration` is still
CDC, `ApplianceOptions` is still the appliance, and a comment about the CDC dry run is still
correct.

One user-facing string deliberately keeps a retired term: `verify-schema-columns.tsx` tells
the operator to enable CDC, naming the PostgreSQL / SQL Server feature they must actually
switch on. Any other wording leaves the message unactionable. That is the bar for an
exception — the word is genuinely the name of something outside our control.

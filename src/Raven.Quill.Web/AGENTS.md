# Raven.Quill.Web

Frontend for Raven Quill. It is a Vite + React + TypeScript app and is finally built by Docker through the `Raven.Quill` project.

## Coding Mindset

- First read nearby code and follow existing project patterns. Prefer project libraries, generated clients, hooks, shadcn components, and established helpers over custom solutions.
- Keep code boring, readable, and explicit. Avoid clever shortcuts, magic values, over-abstraction, and premature optimization.
- Let code be self-explanatory. Add a comment only for non-obvious _why_ (intent, edge cases, workarounds), never to restate what the code already shows. Prefer clear names over explanatory comments.
- Do not declare a `let` only to assign it inside `try`/`catch`. Extract a small helper that returns from the `try` (and throws a friendly error in `catch`) so the caller keeps a `const`.
- Avoid `useEffect` unless it is genuinely needed. Prefer derived state, event handlers, React Query, React Hook Form, Zod, and existing framework mechanisms.
- Do not add `useMemo`/`useCallback` just for routine render optimization; React Compiler is enabled.
- API code should use the generated server API by default. Add custom services only when generated API is not enough.
- Choose between `useQuery` and `useMutation` based on frontend behavior, not only the HTTP verb. Prefer `useQuery` for read-only data fetching when query features such as caching, deduplication, `enabled`, and `refetch` fit well, even if the endpoint is technically `POST`.
- Use `useMutation` for writes and side effects, and also for imperative or user-triggered reads when mutation-style execution is a better fit than query-style state management.
- Forms should use React Hook Form + Zod. If a reusable field is missing, add a generic form component instead of solving it only in one view.
- Prefer shadcn components. Add missing shadcn components with `pnpm dlx shadcn@latest add <component>`. If shadcn does not cover the need, create a project component in the appropriate `src/components` area.
- Use Tailwind for styling, but move hard-to-read style combinations into CSS/classes. Components must work in both light and dark themes; avoid one-theme hardcoded colors.
- Keep components focused and reasonably small. If a component or hook grows awkward, step back and simplify the design before continuing.
- Put reusable hooks/helpers in shared project utilities only when there is a realistic second use.
- Boolean names must clearly read as booleans, e.g. `is`, `has`, `can`, `should`, `was`.
- Use kebab-case filenames, PascalCase React components, `@/*` imports, and `SCREAMING_SNAKE_CASE` module constants.
- Install new dependencies only when they remove real complexity. Prefer popular, maintained packages and latest versions.

## Glossary

One concept, one word, on every surface. These are the product's terms — use them in
every label, heading, menu item, description, toast, `aria-label` and page title.

| Term                  | Means                                        | Never                                              |
| --------------------- | -------------------------------------------- | -------------------------------------------------- |
| **App**               | a configured application                     | "Application"                                      |
| **Data source**       | the source database and its ingestion        | "CDC" as a product noun                            |
| **Dashboard API key** | the single dashboard credential              | "operator API key", "operator key", bare "API key" |
| **Quill**             | the deployment                               | "appliance", "this Quill instance"                 |
| **Sync**              | the running pipeline (performance, progress) | "CDC performance", "CDC feed"                      |

Sentence case throughout, as elsewhere in the UI ("IP configuration", "Data source").
**Quill** is a proper noun and stays capitalised.

`src/lib/vocabulary.test.ts` enforces this. It parses each file and checks only string
literals and JSX text, so internal code comments and identifiers keep the technical
names of the mechanisms they describe — `CdcSinkConfiguration` is still CDC, and a
comment about the CDC dry run is still correct. Only what the operator reads is
governed here.

Adding an allowlist entry to that test is the wrong first move. It is right only when
a word is genuinely the name of something outside our control — the one current entry
covers an instruction to enable the PostgreSQL / SQL Server CDC feature, where any
other wording leaves the operator unable to act.

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
- Typography goes through `<Heading>`/`<Text>` (`src/components/typography.tsx`), and `<SectionHeader>` for a section title with its description and action, instead of hand-written `text-*`/`font-*` classes. See the header comment in `typography.tsx` for the scale.
- Keep components focused and reasonably small. If a component or hook grows awkward, step back and simplify the design before continuing.
- Put reusable hooks/helpers in shared project utilities only when there is a realistic second use.
- Boolean names must clearly read as booleans, e.g. `is`, `has`, `can`, `should`, `was`.
- Test booleans by negation or truthiness, not by comparing to a literal: `!isSaving`, not `isSaving !== true`.
- Use kebab-case filenames, PascalCase React components, `@/*` imports, and `SCREAMING_SNAKE_CASE` module constants.
- Install new dependencies only when they remove real complexity. Prefer popular, maintained packages and latest versions.
- Tag a story `test-only` (not `!dev`) when it exists only for its play function. The Storybook sidebar hides it by default but can still show it, and `pnpm test:storybook` still runs it.

## Viewing and Testing Stories

- `pnpm storybook` serves Storybook at `http://localhost:6106`, together with the `quill-storybook` MCP server. When it is running, use it instead of starting another Storybook. Agents connect to the MCP server when their session starts, so start Storybook first.
- Find the stories that render a component with the MCP `stories-find-by-component` tool, or `stories-changed` for the working-tree changes. Do not guess story ids from file names: a story file can set its own `title`.
- Run story tests with the MCP `test-run` tool, passing `storyId`s for a focused run. It reuses the running Storybook, so it takes a few seconds instead of the ~20 s of `pnpm exec vitest run --project=storybook <story file>`. `pnpm test:storybook` runs all stories, including the widget package.
- Without the MCP tools, run the same tools from the shell, e.g. `pnpm storybook:tools test run --input '{"stories":[{"storyId":"apps-agents--default"}]}'` or `pnpm storybook:tools stories find-by-component --componentPaths '["src/pages/apps/app-agents.tsx"]'`. `pnpm storybook:tools --help` lists them. Without a running Storybook they load its config themselves, which is slower.
- Take a screenshot with `pnpm storybook:screenshot <story-id> [--out file.png] [--theme dark] [--width 1440] [--height 900]`, then read the PNG. It needs the running Storybook, waits until the story and its play function have finished, captures the whole page including what is below the fold, prints browser console errors and exits with code 1 when the story failed. It takes about 3 seconds.
- Do not open stories in an embedded agent browser (such as the in-app browser pane). It cannot register the MSW service worker, so every story that mocks the API fails with "The API mocks cannot run". The MCP `stories-preview` tool returns only a link, not an image. Open a story alone at full width with `/iframe.html?id=<story-id>&viewMode=story`.
- This file wins over the generic guidance of `get-storybook-story-instructions`. Mock API requests with MSW through `mockHandlers` (`src/mocks`), not with `sb.mock`.
- Page stories render in `AppShellMain`, like the app. Set `parameters.page.title` only when the route shows its title, i.e. has no `isPageTitleHidden`. A bare page (`bare: true`) has no title.
- Screenshots and story tests run in the installed Google Chrome (Playwright `channel: "chrome"`), so no `playwright install` is needed.
- A spinner with `aria-label="Loading"` can be intended: some stories mock a request that never resolves to show the loading state.

## Glossary

Product vocabulary is a UX contract: one concept, one word, on every surface. See
[GLOSSARY.md](GLOSSARY.md) and use those terms in every user-visible label, heading, menu
item, description, toast, `aria-label` and page title.

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
- Use kebab-case filenames, PascalCase React components, `@/*` imports, and `SCREAMING_SNAKE_CASE` module constants.
- Install new dependencies only when they remove real complexity. Prefer popular, maintained packages and latest versions.

## Glossary

Product vocabulary is a UX contract: one concept, one word, on every surface. See
[GLOSSARY.md](GLOSSARY.md) and use those terms in every user-visible label, heading, menu
item, description, toast, `aria-label` and page title.

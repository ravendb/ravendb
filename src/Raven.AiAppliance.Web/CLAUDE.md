# Raven.AiAppliance.Web

Frontend for Raven AI Appliance. It is a Vite + React + TypeScript app and is finally built by Docker through the `Raven.AiAppliance` project.

## Coding Mindset

- First read nearby code and follow existing project patterns. Prefer project libraries, generated clients, hooks, shadcn components, and established helpers over custom solutions.
- Keep code boring, readable, and explicit. Avoid clever shortcuts, magic values, over-abstraction, and premature optimization.
- Avoid `useEffect` unless it is genuinely needed. Prefer derived state, event handlers, React Query, React Hook Form, Zod, and existing framework mechanisms.
- Do not add `useMemo`/`useCallback` just for routine render optimization; React Compiler is enabled.
- API code should use the generated server API by default. Add custom services only when generated API is not enough.
- For GET endpoints, add query helpers following existing query patterns. Use `useQuery` with `enabled` and `refetch` for action-triggered reads; use `queryClient.fetchQuery` only when it clearly fits better.
- For writes and side effects, use `useMutation`.
- Forms should use React Hook Form + Zod. If a reusable field is missing, add a generic form component instead of solving it only in one view.
- Prefer shadcn components. Add missing shadcn components with `pnpm dlx shadcn@latest add <component>`. If shadcn does not cover the need, create a project component in the appropriate `src/components` area.
- Use Tailwind for styling, but move hard-to-read style combinations into CSS/classes. Components must work in both light and dark themes; avoid one-theme hardcoded colors.
- Keep components focused and reasonably small. If a component or hook grows awkward, step back and simplify the design before continuing.
- Put reusable hooks/helpers in shared project utilities only when there is a realistic second use.
- Boolean names must clearly read as booleans, e.g. `is`, `has`, `can`, `should`, `was`.
- Use kebab-case filenames, PascalCase React components, `@/*` imports, and `SCREAMING_SNAKE_CASE` module constants.
- Install new dependencies only when they remove real complexity. Prefer popular, maintained packages and latest versions.

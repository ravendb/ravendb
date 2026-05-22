# Raven.AiAppliance.Web

Short rules for AI agents working in this project.

## Purpose

Frontend for Raven AI Appliance. It is a Vite + React + TypeScript app and is finally built by Docker through the `Raven.AiAppliance` project.

## Key Files

- Packages and scripts: `package.json`
- TypeScript config: `tsconfig.json`, `tsconfig.app.json`, `tsconfig.node.json`
- shadcn config: `components.json`

## Structure

- `src/api` - API clients and service calls.
- `src/components/auth` - auth components and context.
- `src/components/form` - reusable form components used in multiple places.
- `src/components/shadcn` - components added by shadcn.
- `src/components/shadcn/ui` - shadcn UI primitives.
- `src/lib` - shared helpers, hooks, clients.
- `src/pages` - route pages.
- `src/app.tsx`, `src/routes.tsx`, `src/main.tsx` - app shell, routing, entry.

## Conventions

- Use kebab-case file names: `setup-connect.tsx`, `form-input.tsx`.
- Use PascalCase for React components.
- Use the `@/*` alias for imports from `src`.
- Put shared form UI in `src/components/form`.
- Put new shadcn-generated files only under `src/components/shadcn`.
- Treat shadcn files as external generated code; avoid editing them unless necessary.

## Commands

Run from this directory:

- Install: `pnpm install`
- Dev server: `pnpm dev`
- Type check: `pnpm typecheck`
- Lint: `pnpm lint`
- Format check: `pnpm format:check`
- Format fix: `pnpm format:fix`
- Production build: `pnpm build`

Keep this file short and easy to edit.

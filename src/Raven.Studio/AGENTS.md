# Raven.Studio

New code is React + TypeScript. It runs inside a legacy Knockout + Durandal shell and is built with webpack. The C# server is in the same repository, so the real rules (endpoints, access, validation, defaults, license checks) can always be read there instead of guessed.

No file paths are listed here on purpose. Search by the names given below.

## Before You Start

- The npm commands work only in the Studio project folder, the one that holds this file. If the session started in another folder, change to it first. After a change run `npm run typecheck`, `npm run lint`, `npx prettier --check <changed files>` and `npx jest <affected spec or folder>`. Run the full `npm test` at the end of a change in shared code (`components/common`, `hooks`, `utils`, the redux slices, the `test` helpers) or in more than one view. A change only in the Studio folder does not need `dotnet build`. For anything else, read the `package.json` scripts instead of looking for tool config files.
- Server typings and the icon font are generated and git-ignored. `npm run restore` creates them. They can be missing (a fresh worktree) or outdated (the files exist, but the server DTOs changed after a pull, a branch switch or your own C# change). Both cases look the same: type errors in files you did not touch, an unknown `Raven.Server.*` or `Raven.Client.*` type or property, or a missing `endpoints`, `configuration` or icon name. When you see this, run `npm run restore` first and check again before you change any code. Never edit generated files, never fix such errors in code and never hide them with a cast.
- Server types are global namespaces (`Raven.Server.*`, `Raven.Client.*`). When a file uses one, give it a short local name at the top (`type X = Raven...` or `import X = Raven...`) instead of repeating the full name. Other imports are absolute from the TypeScript root (`components/...`, `common/...`, `models/...`, `test/...`).

## Always Check: License, Access, Database Type

A change that adds or changes a view, an action or a data load must answer these three questions. A view that works only for an admin with a full license on a non-sharded database is not done.

- **License.** Is the feature, or one of its limits (max count, minimum frequency, allowed types), license dependent? The license status is in the redux store (`licenseSelectors`). Use the existing components and hooks: `FeatureAvailabilitySummary` with `useLimitedFeatureAvailability`, `useProfessionalOrAboveLicenseAvailability` or `useEnterpriseLicenseAvailability`, and `LicenseRestrictedBadge` or `LicenseRestrictedMessage`. A restricted feature stays visible, disabled, with an explanation. The server enforces the same rules in `LicenseManager`. Read it to know the real rule.
- **Access.** There are two axes: security clearance (`ClusterAdmin`, `Operator`, `ValidUser`) and access per database (`DatabaseAdmin`, `DatabaseReadWrite`, `DatabaseRead`). Check three places:
    - the route or menu item (`requiredAccess`), when the whole view needs a level,
    - each action. A control the user cannot use is disabled and wrapped in `AccessPopover`, which says why. `AccessPopover` does not disable the control: get the flag from `accessManagerSelectors.getCanHandleOperation`,
    - the server endpoint. The `RavenAction` attribute on the handler shows the required `AuthorizationStatus`. Check the GET too, not only the save: a view can open and then fail to load its data.
- **Database type.** Sharded or not, one node or a cluster. The menu item sets `shardingMode`, and `bridgeToReact` takes a sharded or non-sharded view type. Some features do not exist on a sharded database (search the server for `NotSupportedInShardingException`). Data can be per node and per shard, and paging, stats and change notifications behave differently on a sharded database.

Stories and tests must cover the variants that change the UI, each in its own story. `mockStore` has helpers for each of them: `license.with_License` and `with_LicenseLimited`, `accessManager.with_securityClearance` and `with_databaseAccess`, `databases.withActiveDatabase_Sharded`, `_NonSharded_Cluster` and `_NonSharded_SingleNode`.

## Where to Look

- **React**: the `components` folder: `pages`, `common` (shared UI), `hooks`, `services`, `utils`, and the redux slices. **Legacy**: `viewmodels` with HTML views and LESS styles. Do not add new Knockout views.
- **Routing**: menu items define the routes. A React page is wrapped with `bridgeToReact`. A React component inside a Knockout view or dialog uses the `react:` binding. Copy an existing usage.
- **HTTP**: a command class, exposed by a method in a service, used through `useServices()` with `useAsync` or `useAsyncCallback`. Service methods should pass `...args: ConstructorParameters<typeof theCommand>`.
- **Global state** (active database, cluster, license, access): redux, always through `useAppSelector`.
- **Links**: `useAppUrls` or `appUrl`. Do not build URL hashes by hand.
- **Shared code**: `components/common`, `components/hooks`, `components/utils` and `generalUtils`. Most needs are already covered: form fields, virtual table, popovers, loading skeletons, confirm dialogs, and the formatting of sizes, numbers, dates and durations.
- **Tests**: the `test` folder holds render helpers, mock services, the mock store and typed stubs.

## Code Rules

- Avoid `as`. First try to fix the type: `satisfies`, a generic, a narrower type, or a complete stub. This also applies to tests and mocks. Keep `as` only when the type system cannot express the case, and never use it to silence an error. `as const` is fine.
- Avoid `useEffect`. Use derived values, event handlers, `useAsync` / `useAsyncCallback` or React Hook Form. A needed effect gets a short comment that says why.
- Search before you create. Find the closest existing component, hook or helper in the shared folders, open it and check if it does the job, maybe with one new prop. Extend it instead of writing a second version next to it. When you do create one that another view may need later, make it general and put it in the shared folders, not in the page folder.
- Tables use `VirtualTable`. Before you write a cell, a column definition or a table hook, check the `virtualTable` folder: it has ready cells, shared column definitions and table hooks. A new common cell goes there too.
- Other things that already exist: `Button` with `variant="link"` for an action that looks like a link, the `Form*` fields (`FormInput`, `FormSelect`, `FormSwitch`, `FormCheckbox` and more in the same file) with React Hook Form and yup, `ConditionalPopover`, `LazyLoad`, `useConfirm`, `useViewSheet`, `ThemeColor`, `studioSettings` for saved UI state, `messagePublisher` for errors.
- Keep one source of truth. If the same rule, list or format exists in two places, move it to a helper. When you fix something, search for the same pattern in similar places and fix it there too.
- No comments that describe the change or repeat the code. Use clear names.
- New module constants are `SCREAMING_SNAKE_CASE`. Most older constants are camelCase. Rename one only when you already change the code that uses it. Boolean names start with `is`, `has`, `can`, `should`.
- Format numbers with `toLocaleString()`. Format dates, durations and sizes with the existing utils (`generalUtils`: `formatUtcDateAsLocal`, `formatDuration`, `formatBytesToSize` and others), and check if the value is local time or UTC.
- Defaults and validation rules belong to the server. Do not hardcode a copy when an endpoint returns the value, and match the C# validation.
- Async code: one failed node or shard must not break the view, a stale response must not replace a newer one, and a request that is still pending is not sent again.
- The UI must work in every theme. Use theme variables, not fixed colors.
- The UI must look good on every screen from 720p (1280x720) up. Most users have 1080p. A view never gets the whole screen: the shell takes the main menu (280 px, open by default), the navbar and the footer, and the view has a 30 px margin on each side. On 720p this leaves about 940 px of width for the content, on 1080p about 1580 px.
- Values that users enter and values from the server can be much longer than the sample data. They must not break the layout.

## Migrating a Knockout View to React

- The old view is the specification. Compare it feature by feature: columns, links, tooltips, empty and error states, access and license checks, sharded behavior.
- When the React view is complete, remove the old view model, its HTML, its LESS and the imports of them.

## Tests and Stories

- A spec sits next to the component and is built on its stories: `composeStories` with `rtlRender`. A story sets its data through `mockServices` and `mockStore`.
- A view gets one general story and one small story per case. The general story (`Default`) takes args, so you can try any combination in the Storybook controls. A case story sets only the `args` that differ. A story takes nothing from another story, only from the meta (the default export). So put `render`, the default `args`, the `argTypes` and the decorators in the meta, not in `Default`.
- Every case that changes the UI needs its own story: each variant from "Always Check", the empty and error state, and long values. Name the story after its case (`LicenseRestricted`, `Sharded`, `NoAccess`, `Empty`, `LongValues`). The general story does not replace them. Many older stories have only the general story. Add the case stories when you change such a view.
- Args are flags and simple values (`isEmpty`, `hasLongValues`, `databaseAccess`, `databaseType`). Add the `withForceRerender` decorator, so a changed control builds the view again with the new mocks. Use the ready arg types from `storybookTestUtils` (`licenseArgType`, `databaseAccessArgType`, `securityClearanceArgType`, `databaseArgType`).
- When the view loads data first, render it with `rtlRender_WithWaitForLoad`. It waits until the loaders are gone, so the test can use `getBy*` right away.
- A test that prints to the console fails. Fix the cause (for an `act` warning: await the UI with `findBy*` and use the action helpers returned by `rtlRender`). Mock a console method only when the test expects that output, never to hide a warning. Do not raise timeouts to hide a slow or flaky test.
- Keep the tests few and meaningful: test what the user sees after an action, for the main flow and for each variant that changes the UI, including the empty and error state. Do not test yup schemas, types, cases that cannot happen (a value the types forbid) or what a shared component or a library already covers.

## Viewing and Testing Stories

- `npm run storybook` serves Storybook at `http://localhost:6006`, together with the `studio-storybook` MCP server. If it already runs, use it: the port is fixed, so a second one fails. If it does not run and you need it, start it in the background and wait until `http://localhost:6006` answers. The MCP tools load only when the session starts in the Studio folder while Storybook is running, so after you start it yourself, use the shell tools below.
- Find stories with the MCP tools `stories-find-by-component` and `stories-changed`. Call `stories-find-by-component` with `maxDistance: 1` first: the default of 3 often returns dozens of unrelated stories. Raise it only when nothing is found, for a shared component that the stories reach through a wrapper. Without MCP, `npm run storybook-tools -- --help` lists the same tools for the shell, but `stories changed` finds nothing there. Do not use the `docs` tools: they know only the components that have stories. Do not guess story ids from file names: a story file sets its own `title`.
- See a story with `npm run storybook-screenshot -- <story-id>`, then read the PNG. Run it without arguments to see the options. It needs the running Storybook and the installed Google Chrome, and exits with code 1 when the story or its play function failed. Pass `--viewport-only` when the layout depends on the viewport height. Take screenshots of the case stories, not of the general story. If a case you need has no story, add one. To try a combination quickly, pass `--args name=value` (repeat it for more args, write arrays and objects as JSON). The script fails and lists the story args when an arg does not exist or its value does not fit.
- A story renders only the view, without the shell. So the screenshot does not use the full screen size: it uses the space that the view gets inside the shell, on a 1080p screen by default. After a layout change, check the smallest supported screen too with `--screen 720p`. Always check the long values story there: most broken layouts show up only with long values on a small screen.
- Run the smoke tests and play functions of stories with `npm run test-storybook -- <story file or folder>`. It uses the running Storybook.
- This file wins over the generic guidance of the MCP `get-storybook-story-instructions` tool. Stories mock their data through `mockServices` and `mockStore`, not through `sb.mock` or MSW.

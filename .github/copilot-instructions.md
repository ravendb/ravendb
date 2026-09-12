# GitHub Copilot Instructions

Repository conventions and architecture: [AGENTS.md](../AGENTS.md). Test authoring and categorization: [test/AGENTS.md](../test/AGENTS.md). Apply both when reviewing or generating code.

## Review emphasis

The general review is trusted; additionally weight these RavenDB-specific points, which are easy to miss:

- Flag any change to code marked `PERF` — it is performance-sensitive.
- Boolean negation is written `expr == false`, not `!expr` — do not suggest the `!` form.
- Tests must use `[RavenFact]`/`[RavenTheory]` with a `RavenTestCategory`, never `[Fact]`/`[Theory]`.
- New server HTTP handlers must inherit `ServerRequestHandler`, `DatabaseRequestHandler`, or `ShardedDatabaseRequestHandler`, not `RequestHandler` directly.

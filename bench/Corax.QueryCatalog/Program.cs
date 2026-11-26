#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Corax.Querying.Planning;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Indexes;
using FastTests;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Tests.Infrastructure;

namespace Corax.QueryCatalog;

public static class Program
{
    public static async Task Main(string[] args)
    {
        using var helper = new ConsoleTestOutputHelper();
        await using var gen = new CoraxCatalogGenerator(helper);
        string outPath = args.Length > 0 ? args[0] : "corax-query-catalog.md";
        await gen.Generate(outPath);
        Console.WriteLine($"Wrote {outPath}");
    }
}

public sealed class Item
{
    public string Id { get; set; }
    public string Name { get; set; }
    public string City { get; set; }
    public int Age { get; set; }
    public double Score { get; set; }
    public DateTime Created { get; set; }
    public string[] Tags { get; set; }
    public double Lat { get; set; }
    public double Lon { get; set; }
    public float[] Embedding { get; set; }
}

public sealed class Items_Index : AbstractIndexCreationTask<Item>
{
    public Items_Index()
    {
        Map = items => from i in items
                       select new { i.Name, i.City, i.Age, i.Score, i.Created, i.Tags };
    }
}

/// <summary>Like <see cref="Items_Index"/> but with a compound field on (City, Age) so
/// <c>CompoundKeyLookup</c> and <c>CompoundSortedScan</c> can fire.</summary>
public sealed class Items_Compound : AbstractIndexCreationTask<Item>
{
    public Items_Compound()
    {
        Map = items => from i in items
                       select new { i.Name, i.City, i.Age, i.Score, i.Created, i.Tags };
        CompoundField(i => i.City, i => i.Age);
    }
}

/// <summary>Like <see cref="Items_Index"/> plus a spatial field (<c>Coordinates</c>) and a vector field
/// (<c>Embedding</c>) so the catalog can exercise <c>spatial.within(...)</c> and <c>vector.search(...)</c>.
/// Spatial resolves through <c>PostFilterMatch</c> and vector through the vector match (approximate NN scan),
/// so their plans differ from the term-leaf entries above.</summary>
public sealed class Items_Geo : AbstractIndexCreationTask<Item>
{
    public Items_Geo()
    {
        Map = items => from i in items
                       select new
                       {
                           i.Name,
                           i.City,
                           i.Age,
                           i.Tags,
                           Coordinates = CreateSpatialField(i.Lat, i.Lon),
                           Embedding = CreateVector(i.Embedding),
                       };
    }
}

/// <summary>
/// Regenerates <c>corax-query-catalog.md</c> against the live engine. For each catalog query it runs the
/// RQL with <c>include timings()</c> over several parameter sets, captures <see cref="QueryTimings.QueryPlan"/>,
/// and dumps every compiled plan variant (strategy, decision trail, generated C#). Compiled variants are read
/// from the index's internal <c>SharedPlanCache</c> via reflection, then enumerated through <see cref="PlanCache.Snapshot"/>.
/// </summary>
public sealed class CoraxCatalogGenerator : RavenTestBase
{
    public CoraxCatalogGenerator(Xunit.ITestOutputHelper output) : base(output)
    {
    }

    private const int DocCount = 50_000;
    private const string IndexName = "Items/Index";

    private sealed record ParamSet(string Label, string Description, Action<IRawDocumentQuery<Item>> Apply);

    private sealed record CatalogQuery(string Label, string Title, string Rql, string Narrative, ParamSet[] Params);

    public async Task Generate(string outPath)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        Seed(store);
        new Items_Index().Execute(store);
        new Items_Compound().Execute(store);
        new Items_Geo().Execute(store);
        Indexes.WaitForIndexing(store);

        var queries = BuildCatalog();

        // Rendered plan PNGs live in a sibling `corax-plans` directory next to the output .md; the markdown
        // embeds reference them by that relative name. Renders are produced inline below via Graphviz `dot`.
        string plansDir = Path.Combine(Path.GetDirectoryName(Path.GetFullPath(outPath)) ?? ".", "corax-plans");
        Directory.CreateDirectory(plansDir);
        string plansDirName = Path.GetFileName(plansDir);
        int planNumber = 0;

        var sb = new StringBuilder();
        sb.AppendLine(
               $"""
               # Corax query catalog — RQL, query plan, and generated code""

               Generated against `{IndexName}` over **{DocCount:N0}** documents (seed 12345).

               For each query we show:

               * the **RQL** (one query text, run with several parameter sets),
               * the **query plan** per parameter set — the structural shape surfaced by `include timings()` (`QueryTimings.QueryPlan`),
               * the **compiled plan variants** for that query text — strategy, decision trail, and the generated C# (`CompiledPlan.FormattedSource`) for each distinct parameter shape the plan cache produced.

               Varying parameters for the same text is what produces multiple compiled variants: the plan cache keys on a digest of parameter *types*, operand cardinality ordering, sentinel state, and the WHEN-survival mask — not on the raw values.

               The `key` shown next to each variant is the low 64 bits of that shape digest. It captures the *plan shape*, not the query text or field names, so structurally identical queries (e.g. a single string-param leaf with no ORDER BY) share the same low-64 value across different query texts — they do not collide in the cache because lookup is keyed by query text first, then by the full 256-bit digest.

               **Selectivity**: a single compiled plan often adapts to selectivity **at runtime** rather than producing distinct cached variants. The `ShouldSwitchToEntryScan` gate in the generated code chooses between a tree-scan intersection and a per-entry residual scan based on the live accumulator cardinality, so the *same* plan serves both a selective and a non-selective parameter set.

               **Cancellation**: a bitmap fill can pull millions of entries into the accumulator (`CtxFillFromPostingSource`, the tree-scan fill, the lazy-OR materialiser). The query's `CancellationToken` is threaded into those helpers and checked once per ~4096-entry batch, so a cancelled query unwinds within one batch regardless of dataset size instead of only after the op completes. The `Ctx*` IL entry-point signatures are unchanged — the token rides on the `CompiledQueryMatch`, not on every emitted call — so the generated C# below looks identical whether or not a token is in play.

               **Corax 1.0*** engine composed a query as a tree of generic match structs — `BinaryMatch<TInner, TOuter, TOp>` nodes for And/Or/AndNot wrapping `MultiTermMatch`, `TermMatch`, sort matches, and so on. Nesting generic structs this way exploded the JIT'd type count combinatorially, so the gnarliest code in that design existed only to *hide* the explosion: `BinaryMatch` carried a hand-rolled function-pointer vtable (`FunctionTable` of `delegate*<ref BinaryMatch, …>` for Fill/AndWith/Score/Count) populated from a `StaticFunctionCache<TInner, TOuter, TBinaryOperationMarker>` so the concrete generic instantiation could be type-erased back to a single non-generic struct. Control flow lived in those function pointers, threaded through `ref this`, and a query's shape was an opaque runtime object graph — impossible to inspect, cache, or read. The IL pipeline documented here retires that iterator-tree composition entirely: the shape is decided once, cached as a `CompiledPlan`, and emitted as the flat, readable op stream + generated C# shown per query.

               ---
               """);

        foreach (CatalogQuery q in queries)
        {
            using var ctx = JsonOperationContext.ShortTermSingleUse();
            sb.Append("## ").Append(q.Label).Append(" — ").AppendLine(q.Title);
            sb.AppendLine();
            sb.AppendLine("```rql");
            sb.AppendLine(q.Rql);
            sb.AppendLine("```");
            sb.AppendLine();
            sb.AppendLine(q.Narrative);
            sb.AppendLine();

            // The text we actually execute carries `include timings()` so the server computes
            // the plan tree. That same text is the plan-cache key, so we match variants on it.
            string timedRql = q.Rql + " include timings()";

            // One collapsible block per parameter set: rendered dataflow graph + DOT source, generated C#,
            // executed strategy, decision trail (when present), structural plan JSON, and raw timings.
            foreach (ParamSet p in q.Params)
            {
                QueryInspectionNode plan = RunForPlan(store, timedRql, p, out QueryTimings timings);
                QueryInspectionNode compiled = FindNode(plan, "CompiledQuery");

                sb.AppendLine("<details>");
                sb.Append("<summary><b>params: ").Append(p.Label).Append("</b> — ").Append(p.Description).AppendLine("</summary>");
                sb.AppendLine();

                // 1 + 2. Physical dataflow graph (PlanGraphDot, shipped on the plan node). Render to PNG and embed,
                // keeping the DOT source in a collapsed <details>. PlanGraphDot is on the OUTER plan root, so read it
                // from `plan` not `compiled` — otherwise sort-wrapped queries (whose root is a sort wrapper) lose
                // their graph and the rendered dataflow drops the sort tail.
                if (TryGetParam(plan, "PlanGraphDot", out string dot))
                {
                    string dotSrc = dot.TrimEnd('\n');
                    planNumber++;
                    string pngName = $"plan-{planNumber:D2}.png";
                    RenderDotToPng(dotSrc, Path.Combine(plansDir, pngName));

                    sb.Append("![Physical dataflow plan ").Append(planNumber.ToString("D2")).Append("](")
                        .Append(plansDirName).Append('/').Append(pngName).AppendLine(")");
                    sb.AppendLine();
                    sb.AppendLine("<details><summary>DOT source</summary>");
                    sb.AppendLine();
                    sb.AppendLine("```dot");
                    sb.AppendLine(dotSrc);
                    sb.AppendLine("```");
                    sb.AppendLine();
                    sb.AppendLine("</details>");
                    sb.AppendLine();
                }

                // 3. Generated C#. The compiler ALWAYS emits the bitmap-pipeline IL; it is only the executed path
                // when the runtime strategy is the bitmap pipeline. For a non-bitmap strategy this IL is the
                // fallback shape that did NOT run, so we label it as such.
                bool ranBitmap = TryGetParam(compiled, "OptimizationHint", out string executed) == false
                    || executed == "BitmapPipeline";
                if (TryGetParam(compiled, "CSharpSourceFormatted", out string csharp))
                {
                    sb.AppendLine(ranBitmap
                        ? "Generated C#:"
                        : $"Generated C# — **bitmap-pipeline fallback, NOT executed**: this run took the `{executed}` strategy, which is built separately and does not go through this IL. The listing below is the path the planner would have used had it fallen back to the bitmap pipeline:");
                    sb.AppendLine();
                    sb.AppendLine("```csharp");
                    sb.Append(csharp.TrimEnd('\n')).AppendLine();
                    sb.AppendLine("```");
                    sb.AppendLine();
                }

                // 4. Strategy that actually ran (runtime cost gate may differ from cached candidacy), with the
                // cached candidate alongside when they differ.
                if (executed != null)
                {
                    sb.Append("Executed strategy: `").Append(executed).Append('`');
                    if (TryGetParam(compiled, "StrategyCandidate", out string candidate) && candidate != executed)
                        sb.Append(" (cached candidate: `").Append(candidate).Append("`)");
                    sb.AppendLine();
                    sb.AppendLine();
                }

                // 5. Decision trail — accept/reject record per optimization considered. Emitted only when recorded.
                QueryInspectionNode trail = FindNode(plan, "DecisionTrail");
                if (trail?.Children is { Count: > 0 })
                {
                    sb.AppendLine("Decision trail:");
                    sb.AppendLine();
                    foreach (QueryInspectionNode entry in trail.Children)
                    {
                        string verdict = TryGetParam(entry, "Accepted", out string acc) && acc == "True" ? "**accepted**" : "**rejected**";
                        TryGetParam(entry, "Reason", out string reason);
                        sb.Append("- `").Append(entry.Operation).Append("` → ").Append(verdict).Append(": ").AppendLine(reason);
                    }

                    sb.AppendLine();
                }

                // 6. Structural plan as JSON — collapsed by default (it is verbose; the graph is the at-a-glance view).
                sb.AppendLine("<details><summary>Query plan (JSON)</summary>");
                sb.AppendLine();
                sb.AppendLine("```json");
                AppendIndentedJson(sb, ctx, PlanToJson(plan));
                sb.AppendLine();
                sb.AppendLine("```");
                sb.AppendLine();
                sb.AppendLine("</details>");
                sb.AppendLine();

                // 7. Raw `include timings()` durations. Wall-clock numbers are illustrative; the value is seeing
                // WHICH stages the engine timed.
                sb.AppendLine("Query timings (wall-clock, illustrative):");
                sb.AppendLine();
                sb.AppendLine("```");
                using (var ms = new MemoryStream())
                using (var writer = new BlittableJsonTextWriter(ctx, ms))
                {
                    ctx.Write(writer, timings?.ToJson());
                    writer.Flush();
                    sb.AppendLine(Encoding.UTF8.GetString(ms.ToArray()));
                }
                sb.AppendLine("```");
                sb.AppendLine();

                sb.AppendLine("</details>");
                sb.AppendLine();
            }

            sb.AppendLine("---");
            sb.AppendLine();
        }

        File.WriteAllText(outPath, sb.ToString());
        Console.WriteLine($"Rendered {planNumber} plan PNGs -> {plansDir}");
    }

    /// <summary>Renders a Graphviz DOT document to a PNG via the <c>dot</c> CLI. Throws with the full <c>dot</c>
    /// stderr on any failure so a broken render aborts the run rather than producing a silently broken catalog.</summary>
    private static void RenderDotToPng(string dot, string pngPath)
    {
        var psi = new ProcessStartInfo
        {
            FileName = "dot",
            RedirectStandardInput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        psi.ArgumentList.Add("-Tpng");
        psi.ArgumentList.Add("-o");
        psi.ArgumentList.Add(pngPath);

        Process process;
        try
        {
            process = Process.Start(psi);
        }
        catch (Exception e)
        {
            throw new InvalidOperationException(
                $"Failed to start Graphviz 'dot' to render '{pngPath}'. Is graphviz installed and on PATH? {e}", e);
        }

        process.StandardInput.Write(dot);
        process.StandardInput.Close();
        string stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();

        if (process.ExitCode != 0)
            throw new InvalidOperationException(
                $"Graphviz 'dot' exited with code {process.ExitCode} while rendering '{pngPath}'. stderr: {stderr}");
    }

    /// <summary>Serializes <paramref name="djv"/> via the blittable writer, then re-emits it indented for a
    /// human-readable JSON view. The blittable round-trip is the source of truth; System.Text.Json only re-flows whitespace.</summary>
    private static void AppendIndentedJson(StringBuilder sb, JsonOperationContext ctx, DynamicJsonValue djv)
    {
        string compact;
        using (var ms = new MemoryStream())
        using (var writer = new BlittableJsonTextWriter(ctx, ms))
        {
            ctx.Write(writer, djv);
            writer.Flush();
            compact = Encoding.UTF8.GetString(ms.ToArray());
        }

        using JsonDocument doc = JsonDocument.Parse(compact);
        sb.Append(JsonSerializer.Serialize(doc.RootElement, new JsonSerializerOptions { WriteIndented = true }));
    }

    /// <summary>Projects the query plan node tree (same QueryInspectionNode tree the Graphviz renderer consumes)
    /// into a DynamicJsonValue. The bulky generated-C# parameters are dropped (shown in their own ```csharp block).</summary>
    private static DynamicJsonValue PlanToJson(QueryInspectionNode node)
    {
        var json = new DynamicJsonValue { ["Operation"] = node.Operation };

        if (node.Parameters is { Count: > 0 })
        {
            var parameters = new DynamicJsonValue();
            foreach (KeyValuePair<string, string> kv in node.Parameters)
            {
                if (kv.Key is "CSharpSource" or "CSharpSourceFormatted" or "PlanGraphDot")
                    continue;
                parameters[kv.Key] = kv.Value;
            }
            json["Parameters"] = parameters;
        }

        if (node.Children is { Count: > 0 })
        {
            var children = new DynamicJsonArray();
            foreach (QueryInspectionNode child in node.Children)
                children.Add(PlanToJson(child));
            json["Children"] = children;
        }

        return json;
    }

    private static QueryInspectionNode RunForPlan(IDocumentStore store, string timedRql, ParamSet p, out QueryTimings timings)
    {
        using IDocumentSession session = store.OpenSession();
        IRawDocumentQuery<Item> q = session.Advanced.RawQuery<Item>(timedRql).Timings(out timings);
        p.Apply(q);
        _ = q.ToList();
        return (QueryInspectionNode)timings.QueryPlan;
    }

    /// <summary>Depth-first search for the first node whose <c>Operation</c> matches (e.g. the CompiledQuery node
    /// or the DecisionTrail node beneath the outer sort wrapper).</summary>
    private static QueryInspectionNode FindNode(QueryInspectionNode node, string operation)
    {
        if (node == null)
            return null;
        if (node.Operation == operation)
            return node;
        if (node.Children != null)
        {
            foreach (QueryInspectionNode child in node.Children)
            {
                QueryInspectionNode found = FindNode(child, operation);
                if (found != null)
                    return found;
            }
        }

        return null;
    }

    private static bool TryGetParam(QueryInspectionNode node, string key, out string value)
    {
        if (node?.Parameters != null && node.Parameters.TryGetValue(key, out value) && string.IsNullOrEmpty(value) == false)
            return true;
        value = null;
        return false;
    }

    private static CatalogQuery[] BuildCatalog()
    {
        return
        [
            new CatalogQuery("eq", "single term equality",
                "from index 'Items/Index' where Name = $p",
                "The simplest leaf. A selective term and a non-matching term still share one compiled plan: the cache keys on the parameter *type*, not the value.",
                [
                    new ParamSet("hit", "$p=\"alice\"", q => q.AddParameter("p", "alice")),
                    new ParamSet("no-match", "$p=\"zzz\"", q => q.AddParameter("p", "zzz")),
                ]),

            new CatalogQuery("or-chain", "three-way OR",
                "from index 'Items/Index' where City = $c or Name = $n or Age = $a",
                "OR queries always materialise into the bitmap pipeline (no streaming sort drive). The three leaves are lazily OR-ed into the accumulator.",
                [
                    new ParamSet("common", "$c=\"London\", $n=\"alice\", $a=30",
                        q => { q.AddParameter("c", "London"); q.AddParameter("n", "alice"); q.AddParameter("a", 30); }),
                    new ParamSet("rare", "$c=\"Rome\", $n=\"erin\", $a=99",
                        q => { q.AddParameter("c", "Rome"); q.AddParameter("n", "erin"); q.AddParameter("a", 99); }),
                ]),

            new CatalogQuery("not-exists-or", "sentinel collapse",
                "from index 'Items/Index' where City = $c or not exists(Name)",
                "`not exists(Name)` is materialised as `AllEntries ANDNOT Exists(Name)` and lazily OR-ed with `City = $c` (see the `FillAllEntries` / `ANDNOT` / `LazyOrWith` ops). Because every document has a `Name`, the negated-exists branch evaluates to empty at runtime, so the result equals `City = $c` — but the engine computes this at runtime; it does not statically fold the branch away, since the planner does not assume the field is always present.",
                [
                    new ParamSet("london", "$c=\"London\"", q => q.AddParameter("c", "London")),
                    new ParamSet("rome", "$c=\"Rome\"", q => q.AddParameter("c", "Rome")),
                ]),

            new CatalogQuery("between", "numeric range",
                "from index 'Items/Index' where Age between $lo and $hi",
                "A single range leaf. Narrow vs wide bounds change selectivity but not the compiled shape; both run the same range scan.",
                [
                    new ParamSet("narrow", "$lo=40, $hi=42", q => { q.AddParameter("lo", 40); q.AddParameter("hi", 42); }),
                    new ParamSet("wide", "$lo=18, $hi=79", q => { q.AddParameter("lo", 18); q.AddParameter("hi", 79); }),
                ]),

            new CatalogQuery("in", "set membership",
                "from index 'Items/Index' where City in ($a, $b, $c)",
                "IN over a posting-list field. Duplicate values collapse to the same posting source.",
                [
                    new ParamSet("three-distinct", "$a=\"London\", $b=\"Paris\", $c=\"Berlin\"",
                        q => { q.AddParameter("a", "London"); q.AddParameter("b", "Paris"); q.AddParameter("c", "Berlin"); }),
                    new ParamSet("all-same", "$a=\"Rome\", $b=\"Rome\", $c=\"Rome\"",
                        q => { q.AddParameter("a", "Rome"); q.AddParameter("b", "Rome"); q.AddParameter("c", "Rome"); }),
                ]),

            new CatalogQuery("in-single", "list-valued IN parameter",
                "from index 'Items/Index' where City in ($a)",
                "A single list-valued parameter expands to the IN set at runtime. With values it is an ordinary posting-list IN; when `$a` is empty the set is unsatisfiable, so the clause resolves to match-nothing and the query returns no documents. Emptiness is a property of the value set, resolved when the plan is instantiated for these parameters.",
                [
                    new ParamSet("with-values", "$a=[\"London\", \"Paris\"]",
                        q => q.AddParameter("a", new[] { "London", "Paris" })),
                    new ParamSet("empty", "$a=[] (no values → match-nothing)",
                        q => q.AddParameter("a", System.Array.Empty<string>())),
                ]),

            new CatalogQuery("when", "compile-time clause gating",
                "from index 'Items/Index' where when($flag = true, City = $c)",
                "`when(cond, expr)` gates a clause on a constant condition evaluated against the query parameters. When the condition holds the leaf is compiled normally (`City = $c`); when it fails the leaf is dropped entirely — and since it is the only clause, the query collapses to match-all (every document). The two cases compile to **different** plans: the WHEN survival mask is part of the plan-cache key, so each parameter set gets its own compiled plan rather than a runtime branch.",
                [
                    new ParamSet("enabled", "$flag=true -> keep `City = $c`",
                        q => { q.AddParameter("flag", true); q.AddParameter("c", "London"); }),
                    new ParamSet("disabled", "$flag=false -> clause dropped -> match-all",
                        q => { q.AddParameter("flag", false); q.AddParameter("c", "London"); }),
                ]),

            new CatalogQuery("nested-group", "OR of two ANDs",
                "from index 'Items/Index' where (City = $c and Age > $a) or (Name = $n and Age < $b)",
                "Two AND groups OR-ed together. Each group intersects into scratch, then the two scratch bitmaps are OR-ed. No sort drive (top-level OR).",
                [
                    new ParamSet("set1", "$c=\"London\", $a=30, $n=\"alice\", $b=70",
                        q => { q.AddParameter("c", "London"); q.AddParameter("a", 30); q.AddParameter("n", "alice"); q.AddParameter("b", 70); }),
                    new ParamSet("set2", "$c=\"Berlin\", $a=50, $n=\"bob\", $b=25",
                        q => { q.AddParameter("c", "Berlin"); q.AddParameter("a", 50); q.AddParameter("n", "bob"); q.AddParameter("b", 25); }),
                ]),

            new CatalogQuery("and-two", "selectivity flip drives operand order",
                "from index 'Items/Index' where Age > $a and City = $c",
                "One compiled plan serves both parameter sets. `City = $c` fills the accumulator and the `Age >` predicate is applied as an AND. Selectivity is handled at runtime: the `ShouldSwitchToEntryScan` gate switches between a tree-scan intersection and a per-entry residual scan based on the live cardinality.",
                [
                    new ParamSet("age-selective", "$a=78, $c=\"London\"", q => { q.AddParameter("a", 78); q.AddParameter("c", "London"); }),
                    new ParamSet("age-broad", "$a=18, $c=\"London\"", q => { q.AddParameter("a", 18); q.AddParameter("c", "London"); }),
                ]),

            new CatalogQuery("order-by", "FieldSortedScan candidacy vs fallback",
                "from index 'Items/Index' where Age > $a order by Age as long",
                "A range predicate sorted on the same field is a `FieldSortedScan` candidate — the trail shows it **accepted** for both bounds — yet here it **falls back to `BitmapPipeline` for both**, and the lesson is *why*. This query has **no page limit** and the range is the **only** clause: with no `limit` the direct tree-walk has no early-stop, and with no second filter there is no residual to make the walk pay off, so it must produce every matching entry in order. But a single range clause is decoded just as completely by one posting-list fill (the bitmap path), and the cost gate charges the direct walk `entries_to_scan × 64` against that — so the scan is strictly more expensive no matter how selective the bound is. The selective bound ($a=78, ~820 rows) and the broad bound ($a=18, ~all rows) therefore reach the **same** decision; selectivity changes the result size, not the strategy. Contrast `filtered-sort`, which adds `limit 16` (early-stop) and a `City` residual — that is what lets the scan beat the bitmap.",
                [
                    new ParamSet("selective", "$a=78", q => q.AddParameter("a", 78)),
                    new ParamSet("broad", "$a=18", q => q.AddParameter("a", 18)),
                ]),

            new CatalogQuery("filtered-sort", "FieldSortedScan actually executes",
                "from index 'Items/Index' where Age > $a and City = $c order by Age as long limit 16",
                "A range predicate **on the sort field** plus a second equality filter, with a small page. This is the shape where the direct tree scan WINS: `Age > $a` is the sort-driving clause, so the scan walks the `Age` term tree in ascending order and applies `City = $c` as a per-entry residual, stopping as soon as the 16-row page is full. The cost gate estimates entries_to_scan = page(16) / City_pass_rate(~0.2) ≈ 80; that × the 64 entry-scan multiplier (~5,120) is far below the bitmap cost of decoding the whole `Age` range plus the full `City` posting list (~50K+), so `FieldSortedScan` executes. The graph shows the `DirectScan` node as the real producer (solid-green `scan result` edge), with `City = $c` listed as its residual; the bitmap-pipeline candidate ops are omitted entirely because they never ran. Note the C# listing below is flagged as the non-executed bitmap fallback — the direct scan is built separately and never runs this IL. Contrast `bare-sort` (`order by Age` with no WHERE): that is ALSO a `FieldSortedScan` candidate (a full-scan direct sort), but with no filter to narrow the set and no productive limit the cost gate makes it scan all 50K entries, so `entries_to_scan × 64` blows past `bitmap_cost` and the 32,768 cap, and the gate falls back to the bitmap pipeline every time. The difference here is the WHERE filter and the small page, which shrink `entries_to_scan` to ~80 so the scan wins.",
                [
                    new ParamSet("broad-age", "$a=18, $c=\"London\"", q => { q.AddParameter("a", 18); q.AddParameter("c", "London"); }),
                    new ParamSet("selective-age", "$a=70, $c=\"Rome\"", q => { q.AddParameter("a", 70); q.AddParameter("c", "Rome"); }),
                ]),

            new CatalogQuery("and-negation", "AndNot",
                "from index 'Items/Index' where City = $c and Name != $n",
                "A positive leaf intersected with a negated leaf. `City = $c` fills the accumulator, then `Name != $n` is applied as an AndNot against the positive `Name = $n` posting list — no full negated bitmap is built. The same `ShouldSwitchToEntryScan` gate may instead run a per-entry residual scan (the `ResidualScan` method rejects rows whose `Name` equals the analyzed term).",
                [
                    new ParamSet("london-not-alice", "$c=\"London\", $n=\"alice\"", q => { q.AddParameter("c", "London"); q.AddParameter("n", "alice"); }),
                    new ParamSet("paris-not-bob", "$c=\"Paris\", $n=\"bob\"", q => { q.AddParameter("c", "Paris"); q.AddParameter("n", "bob"); }),
                ]),

            new CatalogQuery("all-negated-or", "De Morgan",
                "from index 'Items/Index' where Name != $n or City != $c",
                "An OR of two negations, folded by De Morgan: `Name != $n or City != $c` ≡ `NOT(Name = $n and City = $c)`. The generated code computes the positive intersection `Name = $n AND City = $c` into scratch (`bitmap[2]`), fills all entries into the accumulator, then `AndNotWith` subtracts the scratch — i.e. `AllEntries ANDNOT (Name = $n AND City = $c)`.",
                [
                    new ParamSet("set1", "$n=\"alice\", $c=\"London\"", q => { q.AddParameter("n", "alice"); q.AddParameter("c", "London"); }),
                    new ParamSet("set2", "$n=\"erin\", $c=\"Rome\"", q => { q.AddParameter("n", "erin"); q.AddParameter("c", "Rome"); }),
                ]),

            new CatalogQuery("search", "full-text leaf",
                "from index 'Items/Index' where search(Name, $term)",
                "A `search()` leaf tokenises the term through the analyzer pipeline and fills from a match (`CtxFillFromMatch`). A multi-token term (`alice bob`) expands internally to an OR over the analyzed tokens — handled inside the search match, so it surfaces as one `Fill` op with a higher count, not as separate plan ops.",
                [
                    new ParamSet("single", "$term=\"alice\"", q => q.AddParameter("term", "alice")),
                    new ParamSet("multi", "$term=\"alice bob\"", q => q.AddParameter("term", "alice bob")),
                ]),

            new CatalogQuery("startsWith", "prefix scan",
                "from index 'Items/Index' where startsWith(City, $p)",
                "A prefix predicate scans the term tree from the prefix boundary. A matching prefix vs a non-matching one share the compiled shape.",
                [
                    new ParamSet("lon", "$p=\"Lon\"", q => q.AddParameter("p", "Lon")),
                    new ParamSet("none", "$p=\"Zzz\"", q => q.AddParameter("p", "Zzz")),
                ]),

            new CatalogQuery("compound-sort", "exact + tie-break order",
                "from index 'Items/Index' where City = $c order by City, Age as long",
                "An equality leaf sorted by a two-field key. `City = $c` fills the accumulator (a FieldSortedScan candidate), then a `SortingMultiMatch` applies the (City, Age) comparer on top. Note the CompoundKeyLookup / CompoundSortedScan optimizations are *not* triggered here (`CompoundSortedScan` rejected) — ordering is done by the multi-field sort heap, even though the first sort field is constant within the result. This index has no compound field; see `compound-sorted` for the same query shape on an index that does, where `CompoundSortedScan` is accepted and the sort heap disappears.",
                [
                    new ParamSet("london", "$c=\"London\"", q => q.AddParameter("c", "London")),
                    new ParamSet("rome", "$c=\"Rome\"", q => q.AddParameter("c", "Rome")),
                ]),

            new CatalogQuery("mixed-3-level", "AND of (OR, range)",
                "from index 'Items/Index' where (City = $c or Name = $n) and (Age between $lo and $hi)",
                "A two-level tree: an OR group intersected with a range. `City = $c` fills the accumulator, `Name = $n` is OR-ed into it, then the `Age between` range is AND-ed — with the runtime `ShouldSwitchToEntryScan` gate choosing tree-scan intersection vs. per-entry residual scan (the `ResidualScan` method applies the between bounds). Narrow vs wide bounds run the same plan, differing only in the runtime gate decision.",
                [
                    new ParamSet("narrow-age", "$c=\"London\", $n=\"alice\", $lo=40, $hi=42",
                        q => { q.AddParameter("c", "London"); q.AddParameter("n", "alice"); q.AddParameter("lo", 40); q.AddParameter("hi", 42); }),
                    new ParamSet("wide-age", "$c=\"Berlin\", $n=\"bob\", $lo=18, $hi=79",
                        q => { q.AddParameter("c", "Berlin"); q.AddParameter("n", "bob"); q.AddParameter("lo", 18); q.AddParameter("hi", 79); }),
                ]),

            new CatalogQuery("many-residuals", "four predicates collapse to one entry scan",
                "from index 'Items/Index' where Created between $from and $to and City = $c and Age > $a and Score < $s and Name != $n",
                "The selectivity-driven entry-scan path with *multiple* residuals — and a demonstration that **the same compiled plan switches to the entry scan at different clauses depending on the data**. The plan is fixed: `City = $c` fills the accumulator (~10K entries), then `Created between`, `Age > $a`, `Score < $s` are AND-ed in order, and `Name != $n` is an AndNot tail. Before every AND the `ShouldSwitchToEntryScan` gate asks `bitmapCount * 64 < nextClauseCardinality`; the *first* AND that shrinks the accumulator below that threshold triggers the jump to the per-entry residual tail. **`tight-window`** uses a 12-day `Created` range (`Created` has ~5-6 docs/day across a 9000-day span), so the very first AND collapses the accumulator to ~15 entries and the switch happens right after `Created` (`SwitchedAfterClauses=2`) — `Age`, `Score`, `Name` are the residuals. **`wide-window`** opens `Created` to two full years, so that AND barely dents the ~10K accumulator and the gate stays shut; the selective `Age > 75` is what finally collapses it, so the switch happens one clause later, after `Age` (`SwitchedAfterClauses=3`) — now `Score` and `Name` are the residuals. Same generated `ResidualScan` body (four predicates) in both; only the live cardinality decides where execution leaves the tree-scan pipeline.",
                [
                    new ParamSet("tight-window", "$from=2000-01-01, $to=2000-01-12, $c=\"London\", $a=30, $s=500, $n=\"erin\" (Created collapses first → switch after Created)",
                        q =>
                        {
                            q.AddParameter("from", new DateTime(2000, 1, 1));
                            q.AddParameter("to", new DateTime(2000, 1, 12));
                            q.AddParameter("c", "London");
                            q.AddParameter("a", 30);
                            q.AddParameter("s", 500.0);
                            q.AddParameter("n", "erin");
                        }),
                    new ParamSet("wide-window", "$from=2000-01-01, $to=2002-01-01, $c=\"Paris\", $a=75, $s=200, $n=\"bob\" (Created stays wide → Age collapses → switch after Age)",
                        q =>
                        {
                            q.AddParameter("from", new DateTime(2000, 1, 1));
                            q.AddParameter("to", new DateTime(2002, 1, 1));
                            q.AddParameter("c", "Paris");
                            q.AddParameter("a", 75);
                            q.AddParameter("s", 200.0);
                            q.AddParameter("n", "bob");
                        }),
                ]),

            new CatalogQuery("exists", "field-presence leaf",
                "from index 'Items/Index' where exists(Tags)",
                "A bare `exists(field)` leaf. It does not look at any value — it fills the accumulator with every entry that indexed a `Tags` term at all (`ExistsQuery` / the `Fill … Exists` op). In this dataset every document has tags, so it matches all 50K; on a sparse field it would be the cheap way to find the documents that have the field. There are no parameters — the plan is value-independent — so a single parameter set is shown.",
                [
                    new ParamSet("all", "(no parameters)", _ => { }),
                ]),

            new CatalogQuery("tags-all-in", "array ALL IN — conjunctive membership",
                "from index 'Items/Index' where Tags all in ($a, $b)",
                "`all in` over a multi-valued field requires the document's `Tags` array to contain **every** listed value (conjunction), unlike plain `in` which needs only one (disjunction). The first term fills the accumulator (slot 0); the listed values are then folded into that same slot by a single **`AND-Range`** op — a fill-and-AND loop over the expanded term slots (`Terms=2` = the two listed values), each one **intersected** (`AllIn` ∩) so the accumulator shrinks toward the documents that carry all of them, short-circuiting the moment it empties. With two distinct tags the result is documents tagged with both; repeating a tag collapses to the single-term set (`Tags all in (red, red)` ≡ `Tags = red`), which is why the duplicate set returns more documents than the distinct one.",
                [
                    new ParamSet("two-distinct", "$a=\"red\", $b=\"green\"",
                        q => { q.AddParameter("a", "red"); q.AddParameter("b", "green"); }),
                    new ParamSet("duplicate", "$a=\"red\", $b=\"red\" (collapses to Tags = red)",
                        q => { q.AddParameter("a", "red"); q.AddParameter("b", "red"); }),
                ]),

            new CatalogQuery("bare-sort", "ORDER BY with no WHERE",
                "from index 'Items/Index' order by Age as long",
                "A pure ordering with no filter. `FieldSortedScan` IS still a structural candidate — the trail shows it **accepted** — because the `Age` tree can be walked in sorted order directly (a full-scan direct sort). But candidacy is not execution: the per-execution cost gate (`scannedEntries × 64 < bitmapCost && scannedEntries ≤ 32,768`) rejects it here. With no WHERE to narrow the set and no `limit` to cap the walk, the direct scan would have to read **all 50,000** entries (`ScannedEntries: 50000`), and `50000 × 64` is both far above `bitmapCost` and over the 32,768 hard cap — so it falls back to `BitmapPipeline`: fill all entries, then a `SortingMatch` heap on `Age`. Hence `Executed strategy: BitmapPipeline (cached candidate: FieldSortedScan)`. Contrast `filtered-sort` (`City = $c and Age > $a order by Age limit N`), where a WHERE narrows the driving set and a `limit` caps the walk, so the same cost gate **accepts** and `FieldSortedScan` actually executes. No parameters.",
                [
                    new ParamSet("all", "(no parameters)", _ => { }),
                ]),

            new CatalogQuery("compound-key", "CompoundKeyLookup actually fires",
                "from index 'Items/Compound' where City = $c and Age = $a",
                "Run against `Items/Compound`, which declares a Corax compound field on **(City, Age)**. Two equality clauses whose fields are exactly that compound pair — and which together ARE the whole query — collapse into a **single composite-key term lookup**: the engine builds one `compound(City,Age)` key and does a single term seek, instead of filling `City = $c` (~10K) and intersecting `Age = $a` (~800). `Executed strategy: CompoundKeyLookup` confirms this fired; on the plain `Items/Index` (see `and-two`/`compound-sort`) the same shape is always rejected for lack of a compound field. **Read the generated C# carefully**: the compiler always emits the bitmap-pipeline IL (Fill `Age` + AND `City`) from the plan template, but that is NOT what ran here — the compound-key seek is built separately and never goes through this IL, which is why the block is flagged as a non-executed fallback and the graph nodes carry **no `count=`/`ms=`** (those ops never executed). Contrast the two param sets: identical plan, different key value.",
                [
                    new ParamSet("london-40", "$c=\"London\", $a=40", q => { q.AddParameter("c", "London"); q.AddParameter("a", 40); }),
                    new ParamSet("rome-55", "$c=\"Rome\", $a=55", q => { q.AddParameter("c", "Rome"); q.AddParameter("a", 55); }),
                ]),

            new CatalogQuery("compound-sorted", "CompoundSortedScan — cost gate flips on driving selectivity",
                "from index 'Items/Compound' where City = $c and Age > $a and Score > $s order by Age as long",
                "Run against `Items/Compound`, which declares a compound field on **(City, Age)**. The clauses split three ways: `City = $c` is the driving equality (compound field 1), `Age > $a` is a range on the sort field (compound field 2 — the `DirectScanCandidate` flag, without which the planner never even considers a sorted scan), and `Score > $s` is a **residual** — it touches neither compound member, so it cannot be answered by the ordered tree walk and must be tested per scanned entry by reading that entry's stored `Score` (an `EntryTermsReader`, costed at ~64× a posting decode). Together `City = $c` + sort field `Age` form the compound pair, so the decision trail records `CompoundSortedScan` **accepted** — but that is only structural candidacy. Whether it actually runs is decided **per execution** by the cost gate `entriesToScan × 64 < bitmapCost && entriesToScan ≤ 32,768`.\n\nTwo subtleties make this query a clean demonstration. First, **the residual is what engages the gate at all**: with no residual the compound walk reads no stored entries and is unconditionally cheaper than build-bitmap-then-sort, so `CompoundFieldCostEffective` short-circuits to `true` and *both* params would scan (there would be no flip). Adding `Score > $s` forces the per-entry stored-field read, so the gate has to weigh the over-scan. Second, **`entriesToScan` reduces to the driving cardinality here**: the estimator would inflate it by the residual's pass-rate, but only when the most selective residual is *below* the index entry count — and both range residuals (`Age > $a`, `Score > $s`) are estimated at the full ~50K, so that branch is skipped and `entriesToScan = resultsWanted = min(drivingCardinality, pageSize)`. **This query has no `limit`**, so `pageSize` is unbounded and `resultsWanted` is just the driving cardinality — which is exactly what lets driving selectivity reach the gate. (Had we kept a small `limit`, `resultsWanted` would be capped at the limit for *both* params, the gate would see identical inputs, and there would be no flip — so the absence of a limit is load-bearing, not incidental.) `bitmapCost` is the summed estimated cardinality of all clauses (`card(City=$c) + ~50K + ~50K`). The two parameter sets land on opposite sides:\n\n* **`london-bitmap`** (`$c=London`): `City = London` matches ~9.9K docs, so `entriesToScan ≈ 9,943`, and `9,943 × 64 ≈ 636K` dwarfs `bitmapCost ≈ 110K`. The gate fails → `Executed strategy: BitmapPipeline (cached candidate: CompoundSortedScan)`. The displayed C# is the bitmap-pipeline fallback that actually ran.\n* **`vatican-scan`** (`$c=Vatican`): `Vatican` is a deliberately rare city — only ~200 docs versus ~10K for each of the other five (see `Seed`). Now the recorded gate reads `entries_to_scan(200) × 64 < bitmap_cost(100200)` — `12,800 < 100,200`, and 200 is far under the 32,768 cap. The gate passes → `Executed strategy: CompoundSortedScan` **actually fires**: the engine walks the `(Vatican, Age)` compound subtree in `Age` order, reads each entry's `Score` to apply the residual, and emits survivors — instead of building the two ~50K range bitmaps, intersecting, then sorting.\n\nThis is the headline lesson of the introspection output: **`StrategyCandidate` is what the planner is allowed to do; `Executed strategy` is what the cost gate actually chose** — and the deciding input is the selectivity of the driving equality, re-measured against the bound value on every execution. One compiled plan serves both sets; only the parameter moves the gate. (Contrast `compound-key`, where `CompoundKeyLookup` has no cost gate and always fires; and note that for `vatican-scan` the sorted compound scan is built separately and does not go through the bitmap IL shown for the `london-bitmap` variant.)",
                [
                    new ParamSet("london-bitmap", "$c=\"London\", $a=18, $s=500 → BitmapPipeline (driving ~10K)", q => { q.AddParameter("c", "London"); q.AddParameter("a", 18); q.AddParameter("s", 500.0); }),
                    new ParamSet("vatican-scan", "$c=\"Vatican\", $a=18, $s=500 → CompoundSortedScan fires (driving ~200)", q => { q.AddParameter("c", "Vatican"); q.AddParameter("a", 18); q.AddParameter("s", 500.0); }),
                ]),

            new CatalogQuery("spatial-within", "standalone spatial circle",
                "from index 'Items/Geo' where spatial.within(Coordinates, spatial.circle($r, $lat, $lon, 'miles'))",
                "A bare `spatial.within` over the `Coordinates` field on `Items/Geo` (lat/lon indexed via `CreateSpatialField`). A single spatial clause compiles to a `SpatialMatch` — the circle-containment geometry test — and the bitmap pipeline fills the accumulator straight from it: the `Fill` node's `Dispatch=Match` (`CtxFillFromMatch`) means there is no term posting list to seed from, so the `SpatialMatch` itself *is* the match source and every entry it accepts is added to slot 0. `Executed strategy: BitmapPipeline`. The plan surfaces the resolved geometry — `Circle(Pt(0,0), 0.9° / 96.56km)` for the 60-mile circle — and the geohash approximation `Error`. A wider radius simply lets more entries pass the test: same plan, larger result. (Note: this is *not* the dedicated all-entries post-filter bypass. `InstantiateAllEntriesPostFilter` fires only when the resolver produces **zero** bitmap-pipeline clauses — `IsAllEntries`, i.e. `execList.Count == 0` — building a `PostFilterMatch` straight over an `AllEntriesMatch` with no compiled query. A single spatial leaf still resolves to one execution, so it takes the normal `BitmapPipeline` shown here, with the `SpatialMatch` as the `CtxFillFromMatch` source.)",
                [
                    new ParamSet("origin-60mi", "$r=60, $lat=0, $lon=0 (60-mile circle at origin)",
                        q => { q.AddParameter("r", 60.0); q.AddParameter("lat", 0.0); q.AddParameter("lon", 0.0); }),
                    new ParamSet("origin-300mi", "$r=300, $lat=0, $lon=0 (wider circle, more hits)",
                        q => { q.AddParameter("r", 300.0); q.AddParameter("lat", 0.0); q.AddParameter("lon", 0.0); }),
                ]),

            new CatalogQuery("spatial-filtered", "term filter AND spatial circle (\"vegan restaurants in this area\")",
                "from index 'Items/Geo' where Tags = $tag and spatial.within(Coordinates, spatial.circle($r, $lat, $lon, 'miles'))",
                "The canonical \"find all <category> in <area>\" query: a category equality (`Tags = $tag`, standing in for \"vegan\") intersected with a spatial circle (the \"given area\"). Here the term clause fills the accumulator from its posting list (the `Fill` node's `Dispatch=Term`, `FieldName=Tags`, ~21,884 entries), and the spatial clause is applied as a `SpatialMatch` filter on top of that fill (the `SpatialMatch` node following the `Fill[Term]`). This is the efficient shape: the cheap `Tags` posting list narrows the candidate set first, and the (relatively expensive) per-entry circle-containment test only runs on documents that already carry the tag, rather than over all 50K entries. The generated C# below is the term-leaf bitmap fill; the spatial filter is layered on top of it (it is not part of the emitted IL).",
                [
                    new ParamSet("red-origin", "$tag=\"red\", $r=300, $lat=0, $lon=0",
                        q => { q.AddParameter("tag", "red"); q.AddParameter("r", 300.0); q.AddParameter("lat", 0.0); q.AddParameter("lon", 0.0); }),
                    new ParamSet("blue-offset", "$tag=\"blue\", $r=300, $lat=20, $lon=-15",
                        q => { q.AddParameter("tag", "blue"); q.AddParameter("r", 300.0); q.AddParameter("lat", 20.0); q.AddParameter("lon", -15.0); }),
                ]),

            new CatalogQuery("vector-search", "standalone vector.search (approximate nearest-neighbour)",
                "from index 'Items/Geo' where vector.search(Embedding, $q)",
                "A bare `vector.search` over the `Embedding` field (2-D unit vectors indexed via `CreateVector`). This does not fill a term bitmap: it runs an approximate nearest-neighbour scan over the vector index and ranks every matching document by cosine similarity to the query vector `$q`, so results come back ordered by `@index-score` (similarity) even with no explicit `order by score()`. Since the embeddings are unit vectors at a fixed angle, querying with `[1,0]` (\"east\") ranks the documents nearest angle 0 first and `[0,1]` (\"north\") ranks those near 90° first — the *same* compiled plan; only the query vector moves the ranking. `numberOfCandidates` / `minimumSimilarity` are left at their defaults here.",
                [
                    new ParamSet("east", "$q=[1,0]", q => q.AddParameter("q", new[] { 1f, 0f })),
                    new ParamSet("north", "$q=[0,1]", q => q.AddParameter("q", new[] { 0f, 1f })),
                ]),

            new CatalogQuery("vector-filtered", "term filter AND vector.search",
                "from index 'Items/Geo' where City = $c and vector.search(Embedding, $q)",
                "Vector similarity narrowed by an ordinary term filter: `City = $c` restricts the candidate set through the term pipeline, and `vector.search` ranks the survivors by similarity to `$q`. This is the \"nearest matches *that also* satisfy a hard filter\" shape — the term predicate is an exact constraint, the vector clause is the ranking signal. The generated C# below shows the `City = $c` bitmap fill; the vector match consumes that candidate set and re-ranks it by score.",
                [
                    new ParamSet("london-east", "$c=\"London\", $q=[1,0]",
                        q => { q.AddParameter("c", "London"); q.AddParameter("q", new[] { 1f, 0f }); }),
                    new ParamSet("rome-north", "$c=\"Rome\", $q=[0,1]",
                        q => { q.AddParameter("c", "Rome"); q.AddParameter("q", new[] { 0f, 1f }); }),
                ]),

            new CatalogQuery("vector-or-term", "vector.search OR a term equality (vector as an OR-branch leaf)",
                "from index 'Items/Geo' where vector.search(Embedding, $q) or City = $c",
                "A `vector.search` **OR-ed** with an ordinary term equality — contrast `vector-filtered`, where the term clause is an AND constraint that pre-narrows the candidate set. Here the two branches are a **union**: a document qualifies if it is a near neighbour of `$q` OR it lives in city `$c`. The key point is the vector clause's ROLE: inside an OR branch it is an ordinary bitmap-pipeline leaf, NOT a top-level post-filter. The planner only lifts vector/spatial clauses to per-entry post-filters in an AND context (`ApplyPostFilters` runs over the top-level AND); in an OR the vector match must contribute its own matching set to be unioned, so it resolves as a fill-from-match source that is OR-ed with the `City = $c` posting list. (This is exactly the nested-leaf path that `IsPostFilter` must report as `false` — a vector leaf in an OR is not a post-filter.) The two parameter sets move the query vector and the city; the plan shape is identical.",
                [
                    new ParamSet("east-or-london", "$q=[1,0], $c=\"London\"",
                        q => { q.AddParameter("q", new[] { 1f, 0f }); q.AddParameter("c", "London"); }),
                    new ParamSet("north-or-rome", "$q=[0,1], $c=\"Rome\"",
                        q => { q.AddParameter("q", new[] { 0f, 1f }); q.AddParameter("c", "Rome"); }),
                ]),

            new CatalogQuery("spatial-and-spatial", "two spatial circles AND-ed (all-entries fill + both circles post-filter)",
                "from index 'Items/Geo' where spatial.within(Coordinates, spatial.circle($r1, $lat1, $lon1, 'miles')) and spatial.within(Coordinates, spatial.circle($r2, $lat2, $lon2, 'miles'))",
                "Two `spatial.within` circles AND-ed together with no term clause. There is no posting list to seed from, but the planner does **not** take the dedicated all-entries bypass here — that path (`InstantiateAllEntriesPostFilter`, which builds a `PostFilterMatch` straight over an `AllEntriesMatch` with no `CompiledQueryMatch` at all) is reserved for a query whose *only* WHERE content is a single spatial/vector leaf (`exec is { IsAllEntries: true, HasSpatialOrVector: true }`). With two spatial clauses the planner instead emits a trivial one-op bitmap pipeline — the generated C# is a single `CtxFillFromMatch(ctx, cursor, bitmapSlot: 0)` (the `Fill` node's `Dispatch=Match`), which fills slot 0 from the implicit all-entries match because no clause produces a posting list — and then `ApplyPostFilters` layers **both** circles on top as a `SpatialMatch` post-filter chain. So the plan graph renders as `Fill[Match] → SpatialMatch → SpatialMatch → Result` (`Executed strategy: BitmapPipeline`): every document is fed through both circle-containment tests per entry and survives only if it falls inside their intersection. Functionally this is the same all-entries + both-spatials shape as the bypass, just realised through the normal `CompiledQueryMatch` fill rather than the dedicated bypass code path. The two circles are deliberately **different** (distinct centres *and* radii), so the two `SpatialMatch` post-filter nodes carry visibly different `Shape` values in the plan graph — they are two separate geometry tests, not an accidental duplicate of one. The two parameter sets move the circle centres/radii: same plan, the intersection region (and thus the result count) shifts.",
                [
                    new ParamSet("big-and-small", "$r1=500@(0,0), $r2=120@(8,3) (a wide circle AND a small offset circle)",
                        q => { q.AddParameter("r1", 500.0); q.AddParameter("lat1", 0.0); q.AddParameter("lon1", 0.0); q.AddParameter("r2", 120.0); q.AddParameter("lat2", 8.0); q.AddParameter("lon2", 3.0); }),
                    new ParamSet("offset-pair", "$r1=700@(-20,15), $r2=250@(30,-10) (two circles at different centres and radii)",
                        q => { q.AddParameter("r1", 700.0); q.AddParameter("lat1", -20.0); q.AddParameter("lon1", 15.0); q.AddParameter("r2", 250.0); q.AddParameter("lat2", 30.0); q.AddParameter("lon2", -10.0); }),
                ]),

            new CatalogQuery("spatial-distance-sort", "spatial filter ORDER BY spatial.distance (nearest-first)",
                "from index 'Items/Geo' where spatial.within(Coordinates, spatial.circle($r, $lat, $lon, 'miles')) order by spatial.distance(Coordinates, spatial.point($lat, $lon))",
                "The \"nearest matches first\" shape: a `spatial.within` circle narrows the candidate set, and `order by spatial.distance(...)` sorts the survivors by their great-circle distance from the query point. The distance sort is a result-shaping wrapper above the bitmap pipeline — the plan graph renders it as a `Sort` tail node carrying the spatial sort field, the reference point, and the direction (ascending = nearest first). The `SpatialMatch` filter still runs inside the pipeline; the sort only reorders what it produced. Both param sets keep the filter and sort anchored on the same point, so a wider radius simply admits more documents into the same nearest-first ordering.",
                [
                    new ParamSet("origin-300mi", "$r=300, $lat=0, $lon=0 (nearest-first within 300mi of origin)",
                        q => { q.AddParameter("r", 300.0); q.AddParameter("lat", 0.0); q.AddParameter("lon", 0.0); }),
                    new ParamSet("offset-600mi", "$r=600, $lat=20, $lon=-15 (wider radius, offset centre)",
                        q => { q.AddParameter("r", 600.0); q.AddParameter("lat", 20.0); q.AddParameter("lon", -15.0); }),
                ]),

            new CatalogQuery("boost-score", "boosted clauses ORDER BY score() (relevance ranking)",
                "from index 'Items/Index' where boost(City = $c, 10) or boost(Name = $n, 2) order by score()",
                "Two leaves with explicit per-clause `boost(...)` factors OR-ed together, ranked by `order by score()`. Each `boost(leaf, factor)` wraps that leaf's postings in a `BoostingMatch` that scales its score contribution — the plan graph surfaces the factor on the leaf node (`boost x10` on the `City` fill, `boost x2` on the `Name` fill). The `order by score()` is the result wrapper that consumes those scores: the graph renders a `Sort` tail node flagged `rank by score()`. (Boosting also auto-promotes to score ordering even without an explicit `order by score()`, but here it is requested explicitly.) The two parameter sets move which city/name dominate the ranking; the plan shape is identical.",
                [
                    new ParamSet("london-alice", "$c=\"London\", $n=\"alice\"",
                        q => { q.AddParameter("c", "London"); q.AddParameter("n", "alice"); }),
                    new ParamSet("rome-bob", "$c=\"Rome\", $n=\"bob\"",
                        q => { q.AddParameter("c", "Rome"); q.AddParameter("n", "bob"); }),
                ]),

            new CatalogQuery("regex", "regex(...) term scan",
                "from index 'Items/Index' where regex(Name, $p)",
                "A `regex` predicate over the `Name` field. Regex is not a posting-list seek: it resolves through a `TermsProviderMatch` backed by a `RegexTermsProvider`, which walks the field's term dictionary and tests each distinct term against the pattern, unioning the postings of every term that matches. The plan graph renders it as a single pipeline leaf (`Fill` from the terms provider) — there is no per-term IL, the provider streams matching terms at runtime. The two parameter sets use different anchored patterns over the small name vocabulary (alice/bob/carol/dave/erin); the result count follows how many distinct names match.",
                [
                    new ParamSet("starts-a", "$p=\"^a\" (names starting with a → alice)", q => q.AddParameter("p", "^a")),
                    new ParamSet("contains-r", "$p=\"r\" (names containing r → carol, erin)", q => q.AddParameter("p", "r")),
                ]),
        ];
    }

    private static void Seed(IDocumentStore store)
    {
        var cities = new[] { "London", "Paris", "Berlin", "Madrid", "Rome" };
        // A deliberately rare sixth city (~200 docs vs ~10K each for the others). That selectivity lets the
        // cost gate flip CompoundSortedScan from a bitmap fallback to a sorted compound-tree scan.
        const string rareCity = "Vatican";
        const int rareCityDocs = 200;
        var names = new[] { "alice", "bob", "carol", "dave", "erin" };
        var tags = new[] { "red", "green", "blue", "yellow" };
        var rng = new Random(12345);
        using Raven.Client.Documents.BulkInsert.BulkInsertOperation bulk = store.BulkInsert();
        for (int i = 0; i < DocCount; i++)
        {
            // Spatial: scatter over a 100°×100° lat/lon box centred on (0,0) so spatial.within is selective
            // and its result count moves with the radius/centre.
            double lat = rng.NextDouble() * 100 - 50;
            double lon = rng.NextDouble() * 100 - 50;

            // Vector: a 2-D unit vector at a random angle, so cosine similarity is just the angle between
            // vectors — a deterministic, model-free embedding the catalog can rank.
            double angle = rng.NextDouble() * 2 * Math.PI;

            bulk.Store(new Item
            {
                Name = names[rng.Next(names.Length)],
                City = i < rareCityDocs ? rareCity : cities[rng.Next(cities.Length)],
                Age = rng.Next(18, 80),
                Score = rng.NextDouble() * 1000,
                Created = new DateTime(2000, 1, 1).AddDays(rng.Next(0, 9000)).AddSeconds(rng.Next(0, 86400)),
                Tags = new[] { tags[rng.Next(tags.Length)], tags[rng.Next(tags.Length)] },
                Lat = lat,
                Lon = lon,
                Embedding = new[] { (float)Math.Cos(angle), (float)Math.Sin(angle) }
            });
        }
    }
}

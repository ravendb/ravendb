# RavenDB.Analyzers

Roslyn analyzers that catch common RavenDB misuses at compile time.
All rules default to **Warning** (or **Info** where noted) and can be promoted to errors via `.editorconfig`:

```editorconfig
dotnet_diagnostic.RVN004.severity = error
```

## RVN001: Map or Reduce assigned outside constructor

**Triggered by:** assigning the `Map` or `Reduce` property of an index class inside a regular method (not a constructor).

RavenDB reads the `Map` and `Reduce` expression trees at **index registration time**, which happens when the index class is constructed. If the assignment is done in a helper method that the constructor delegates to, or in any method called conditionally after construction, the analyzer cannot guarantee the assignment happens in time and neither can the runtime:

```csharp
// ❌ Bad: Map is assigned in a method, not the constructor
class MyIndex : AbstractIndexCreationTask<Order>
{
    public MyIndex() { Configure(); }

    private void Configure()
    {
        Map = orders => from o in orders   // RVN001
                        select new { o.Id };
    }
}

// ✅ Good: Map is assigned directly in the constructor
class MyIndex : AbstractIndexCreationTask<Order>
{
    public MyIndex()
    {
        Map = orders => from o in orders
                        select new { o.Id };
    }
}
```

This rule also fires for `Reduce` assigned in a method:

```csharp
// ❌ Bad
public void SetReduce()
{
    Reduce = results => ...;    // RVN001
}
```

**Docs:** [Creating and deploying indexes](https://ravendb.net/docs/article-page/latest/csharp/indexes/creating-and-deploying)


---

## RVN002: RavenDB query operator after projection

**Triggered by:** calling any of the following query operators on an `IRavenQueryable<T>` chain that has already called `ProjectInto<T>()` or `Select(...)`:

`Where` · `OrderBy` · `OrderByDescending` · `ThenBy` · `ThenByDescending` · `GroupBy` · `Search` · `Spatial` · `OrderByDistance` · `OrderByDistanceDescending` · `OrderByScore` · `OrderByScoreDescending` · `ThenByScore` · `ThenByScoreDescending` · `MoreLikeThis` · `VectorSearch` · `Filter` · `GroupByArrayValues` · `GroupByArrayContent`

RavenDB translates a LINQ query chain into a single server-side query. Projection (`ProjectInto` / `Select`) changes the element type of the chain: all of the operators above bind to the **source document / index shape** and must therefore appear *before* projection. When called after a projection they silently operate on the wrong type or throw a runtime exception.

```csharp
// ❌ Bad: Where runs on OrderView, not on Order
var q = session.Query<Order>()
    .ProjectInto<OrderView>()
    .Where(x => x.Status == "active");   // RVN002

// ✅ Good: move projection to the end
var q = session.Query<Order>()
    .Where(x => x.Status == "active")
    .ProjectInto<OrderView>();
```

The same applies to Raven-specific operators:

```csharp
// ❌ Bad: Search on a spatial field applies to OrderView, not Order
var q = session.Query<Order>()
    .ProjectInto<OrderView>()
    .Search(x => x.Description, "urgent");   // RVN002

// ❌ Bad: OrderByDistance after Select has no source-shape coordinates to sort on
var q = session.Query<Order>()
    .Select(x => new { x.Id })
    .OrderByDistance(x => x.Location, 51.5, -0.1);   // RVN002

// ✅ Good: all operators before projection
var q = session.Query<Order>()
    .Search(x => x.Description, "urgent")
    .OrderByDistance(x => x.Location, 51.5, -0.1)
    .ProjectInto<OrderView>();
```

**Explicitly not flagged** (legitimately usable after projection): `Include`, `Highlight`, `Statistics`, `Customize`, `Skip`, `Take`.

**Docs:** [Projecting query results](https://ravendb.net/docs/article-page/latest/csharp/client-api/session/querying/how-to-project-query-results)

---

## RVN003: ProjectInto called more than once

**Triggered by:** a second `.ProjectInto<T>()` call on the same query chain.

Calling `ProjectInto` sets an internal flag on the RavenDB query provider. Calling it again unconditionally throws `InvalidOperationException` at runtime. There is no scenario where two `ProjectInto` calls on the same chain are valid.

```csharp
// ❌ Bad: throws InvalidOperationException at runtime
var q = session.Query<Order>()
    .ProjectInto<OrderView>()
    .ProjectInto<OrderView2>();    // RVN003

// ✅ Good: project into one target type only
var q = session.Query<Order>()
    .ProjectInto<OrderView>();
```

---

## RVN004: AbstractIndexCreationTask subclass is missing a Map assignment

**Triggered by:** a class that inherits from `AbstractIndexCreationTask<T>` but has no constructor that assigns the `Map` property.

Every RavenDB index must define what it maps. A class without a `Map` assignment has no index definition and will throw when deployed to a server. This is the most common cause of a "blank index" that compiles fine but fails on first use.

```csharp
// ❌ Bad: no Map assigned anywhere
class OrdersByStatus : AbstractIndexCreationTask<Order>
{
    public OrdersByStatus() { }   // RVN004 reported on the class name
}

// ❌ Bad: no constructor at all (implicit parameterless ctor has no Map)
class OrdersByStatus : AbstractIndexCreationTask<Order>
{
}

// ✅ Good
class OrdersByStatus : AbstractIndexCreationTask<Order>
{
    public OrdersByStatus()
    {
        Map = orders => from o in orders
                        select new { o.Status };
    }
}
```

Note: if the `Map` is assigned in a helper method rather than the constructor body directly, RVN001 fires on the assignment and RVN004 fires on the class. Both rules apply simultaneously because the constructor itself still has no `Map` assignment.

**Docs:** [Creating and deploying indexes](https://ravendb.net/docs/article-page/latest/csharp/indexes/creating-and-deploying)

---

## RVN005: Multi-map index has no AddMap call in any constructor

**Triggered by:** a class that inherits from `AbstractMultiMapIndexCreationTask<T>`, `AbstractMultiMapTimeSeriesIndexCreationTask<T>`, or `AbstractMultiMapCountersIndexCreationTask<T>` but has no constructor that calls `AddMap` or `AddMapForAll`.

Multi-map index classes do not expose a `Map` property: they define their mappings by calling `AddMap<TSource>(...)` in the constructor. Without at least one `AddMap` call the index has no definition and will throw when deployed.

```csharp
// ❌ Bad: no AddMap call
class MultiIndex : AbstractMultiMapIndexCreationTask<Result>
{
    public MultiIndex() { }   // RVN005 reported on the class name
}

// ✅ Good
class MultiIndex : AbstractMultiMapIndexCreationTask<Result>
{
    public MultiIndex()
    {
        AddMap<Company>(companies => from c in companies
                                     select new Result { Name = c.Name });
        AddMap<Employee>(employees => from e in employees
                                      select new Result { Name = e.FirstName });
    }
}
```

Note: `AddMap` calls in a helper method that the constructor delegates to are **not** counted: the same logic as RVN001/RVN004. Move `AddMap` calls into the constructor body directly.

**Docs:** [Multi-map indexes](https://ravendb.net/docs/article-page/latest/csharp/indexes/multi-map-indexes)

---

## RVN006: Multi-map index uses only a single AddMap (Info)

**Triggered by:** a class that inherits from a multi-map index base but calls `AddMap` exactly once across all constructors.

A multi-map index base class is designed to map over **multiple** document types (or time-series/counter names). When only a single `AddMap` is present, the simpler `AbstractIndexCreationTask<TDocument>` form is equivalent and more readable.

This rule has **Info** severity and is purely a style suggestion.

```csharp
// ⚠️ Single AddMap: consider a regular index instead (RVN006)
class MultiIndex : AbstractMultiMapIndexCreationTask<Result>
{
    public MultiIndex()
    {
        AddMap<Company>(companies => from c in companies
                                     select new Result { Name = c.Name });
    }
}

// ✅ Simpler equivalent
class CompanyIndex : AbstractIndexCreationTask<Company>
{
    public CompanyIndex()
    {
        Map = companies => from c in companies
                           select new { c.Name };
    }
}
```

**Docs:** [Multi-map indexes](https://ravendb.net/docs/article-page/latest/csharp/indexes/multi-map-indexes)

---

## RVN007: Query field not present in the index projection (Info)

**Triggered by:** a `Where`, `OrderBy`, `OrderByDescending`, `ThenBy`, `ThenByDescending`, or `Search` lambda on a `session.Query<T, TIndexCreator>()` (or the `indexName:` string overload) that references a field not projected by the index's `Map` expression.

RavenDB translates the LINQ chain into a server-side query against a specific index. If a `Where` or `OrderBy` clause references a field that is not in the index's projection, the server cannot match documents against that field and the query silently returns no results or an error at runtime.

```csharp
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name, o.Status };
    }
}

// ❌ Bad: Price is not in the index projection
var q = session.Query<Order, OrderIndex>()
    .Where(x => x.Price > 100);    // RVN007 on x.Price

// ✅ Good: Name is projected by the index
var q = session.Query<Order, OrderIndex>()
    .Where(x => x.Name == "Acme");
```

The same rule applies to `OrderBy`, `Search`, and the string-based overload:

```csharp
// ❌ Bad: Price is not indexed
var q = session.Query<Order>(indexName: "OrderIndex")
    .OrderBy(x => x.Price);        // RVN007 on x.Price
```

This rule has **Info** severity. It bails silently (no diagnostic) when:
- The index uses `CreateField`, `AsJson`, or `StoreAllFields` (dynamic fields)
- The index inherits from `AbstractJavaScriptIndexCreationTask`
- The `Map` right-hand side is not a lambda expression
- The index class is not in the current compilation
- The `indexName:` string argument is a variable rather than a string literal (string-based overload only)
- The index overrides `IndexName` with a non-literal expression such as a variable or concatenation (string-based overload only — the generic `Query<T, TIndex>()` form always resolves the class directly and is unaffected)

**Docs:** [Querying an index](https://ravendb.net/docs/article-page/latest/csharp/indexes/querying/query-index)

---

## RVN008: Projected field not retrievable under the applied ProjectionBehavior (Info)

**Triggered by:** a `ProjectInto<T>()` or `Select(…)` call on a `session.Query<TSource, TIndex>()` where a projected field is not retrievable under the effective `ProjectionBehavior`.

RavenDB fetches each projected field from the index entry (if the field is stored) or falls back to the source document. This behavior is controlled by `ProjectionBehavior`:

| Behavior | Where fields come from |
|---|---|
| `Default` (or none) | Stored index field → fallback to source document |
| `FromIndex` / `FromIndexOrThrow` | Stored index field only: no document fallback |
| `FromDocument` / `FromDocumentOrThrow` | Source document only: no stored field lookup |

Fields are stored via `Store(x => x.Field, FieldStorage.Yes)`, `Store("FieldName", FieldStorage.Yes)`, `Stores[…] = FieldStorage.Yes`, `StoresStrings["FieldName"] = FieldStorage.Yes`, or `StoreAllFields(FieldStorage.Yes)`. Just appearing in the `Map` projection does **not** store a field.

```csharp
class OrderIndex : AbstractIndexCreationTask<Order>
{
    public OrderIndex()
    {
        Map = orders => from o in orders select new { o.Name };
        Store(x => x.Name, FieldStorage.Yes);
    }
}

class Dto { public string Name { get; set; } public string Ghost { get; set; } }

// ❌ Bad: Ghost is not stored and not on Order; projected value will be null
var q = session.Query<Order, OrderIndex>().ProjectInto<Dto>();   // RVN008 on Ghost

// ✅ Good: Name is stored; Price is on the source document
class Dto2 { public string Name { get; set; } public decimal Price { get; set; } }
var q2 = session.Query<Order, OrderIndex>().ProjectInto<Dto2>();

// ❌ Bad: Price is on source doc, but FromIndexOrThrow disables document fallback
var q3 = session.Query<Order, OrderIndex>()
    .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
    .ProjectInto<Dto2>();   // RVN008 on Price: only Name is stored
```

The same rule applies to `Select` projections when `FromIndex` or `FromIndexOrThrow` is set:

```csharp
// ❌ Bad: Price is not stored; FromIndexOrThrow means no document fallback
var q = session.Query<Order, OrderIndex>()
    .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
    .Select(x => new { x.Name, x.Price });   // RVN008 on x.Price
```

This rule has **Info** severity. It bails silently (no diagnostic) when:
- The index uses `CreateField`, `AsJson`, or other dynamic-field methods
- The index inherits from `AbstractJavaScriptIndexCreationTask`
- A `Store(…)` argument is a variable rather than a lambda or string literal
- A `Customize(x => x.Projection(…))` argument is a variable rather than a `ProjectionBehavior.X` member access
- The index class is not in the current compilation
- `Query<T>()` is called without an index reference (auto-index)

**Docs:** [Projecting query results](https://ravendb.net/docs/article-page/latest/csharp/client-api/session/querying/how-to-project-query-results)

---

## RVN009: Unsupported method call inside index Map/Reduce expression

**Triggered by:** an invocation of a user-defined method inside the lambda body of a `Map`, `Reduce`, or `AddMap` assignment in a RavenDB index class.

RavenDB compiles index `Map` and `Reduce` expressions to server-side IL at **index deployment time**. Only BCL methods (string, Math, etc.), LINQ operators, and Raven-provided helpers (e.g. `LoadDocument`, `CreateField`) can be translated. A user-defined helper method — whether a local method on the index class, a static utility, or an instance method on a user type — cannot be compiled by the expression engine and will cause the index to fail with a runtime exception.

```csharp
// ❌ Bad: MyHelpers.Normalize is user-defined and cannot be translated
class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new { Name = MyHelpers.Normalize(p.Name) };  // RVN009
    }
}

// ✅ Good: inline the operation or use BCL / Raven helpers
class ProductIndex : AbstractIndexCreationTask<Product>
{
    public ProductIndex()
    {
        Map = products => from p in products
                          select new { Name = p.Name.ToLowerInvariant() };
    }
}
```

The same rule applies to `Reduce` and multi-map `AddMap` lambdas:

```csharp
// ❌ Bad: user-defined method in Reduce
Reduce = results => from r in results
                    group r by r.Tag into g
                    select new { Tag = g.Key, Count = Utils.Sum(g) };  // RVN009
```

This rule bails silently (no diagnostic) for:
- Indexes derived from `AbstractJavaScriptIndexCreationTask` (JS indexes cannot be statically analyzed)
- Methods whose containing type is defined in a referenced assembly (BCL, Raven.Client, NuGet packages)

**Docs:** [Map indexes](https://ravendb.net/docs/article-page/latest/csharp/indexes/map-indexes)

---

## RVN010: Unsupported method call inside RavenDB query expression

**Triggered by:** an invocation of a user-defined method inside a lambda passed to a RavenDB LINQ query chain method (`Where`, `OrderBy`, `Select`, `Search`, `ProjectInto`, etc.) on `IRavenQueryable<T>`.

RavenDB translates LINQ query lambdas to RQL before sending them to the server. User-defined methods inside these lambdas cannot be translated and will throw an exception at runtime. Compute derived values before the query, inline the predicate logic, or materialize with `ToList()` first and apply the method client-side.

```csharp
// ❌ Bad: MyFilters.IsActive is user-defined and cannot be sent to the server
var results = session.Query<Order>()
    .Where(o => MyFilters.IsActive(o.Status))   // RVN010
    .ToList();

// ✅ Good: resolve the value before the query
var results = session.Query<Order>()
    .Where(o => o.Status == "Active")
    .ToList();

// ✅ Also good: project first, then apply client-side logic
var results = session.Query<Order>()
    .ToList()
    .Where(o => MyFilters.IsActive(o.Status));
```

The same rule applies to `OrderBy`, `Select`, and other lambda-taking query methods:

```csharp
// ❌ Bad: user-defined method in Select projection
var results = session.Query<Order>()
    .Select(o => new { Score = MyScorer.Compute(o) });  // RVN010
```

This rule applies **only** to `IRavenQueryable<T>` chains (`session.Query<T>()`). Standard LINQ on in-memory collections is not affected.

**Docs:** [Querying in RavenDB](https://ravendb.net/docs/article-page/latest/csharp/client-api/session/querying/how-to-query)

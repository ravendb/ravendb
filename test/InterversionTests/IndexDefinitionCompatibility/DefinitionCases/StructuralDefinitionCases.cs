using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;

namespace InterversionTests.IndexDefinitionCompatibility.DefinitionCases;

internal static class StructuralDefinitionCases
{
    public static IReadOnlyList<DefinitionCase> Create()
    {
        return
        [
            new DefinitionCase(
                "contains-placement/projection-direct",
                conventions => Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, object>
                {
                    Map = docs => from doc in docs
                                  select new
                                  {
                                      doc.Id,
                                      HasMatch = MemoryExtensions.Contains(doc.IntValues, 42)
                                  }
                })),

            new DefinitionCase(
                "contains-placement/query-where",
                conventions => Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, object>
                {
                    Map = docs => from doc in docs
                                  where MemoryExtensions.Contains(doc.IntValues, 42)
                                  select new { doc.Id }
                })),

            new DefinitionCase(
                "contains-placement/query-let",
                conventions => Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, object>
                {
                    Map = docs => from doc in docs
                                  let hasMatch = MemoryExtensions.Contains(doc.IntValues, 42)
                                  select new { doc.Id, HasMatch = hasMatch }
                })),

            new DefinitionCase(
                "contains-placement/query-nested-from-where",
                conventions => Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, object>
                {
                    Map = docs => from doc in docs
                                  from item in doc.Items
                                  where MemoryExtensions.Contains(item.Values, 42)
                                  select new { doc.Id, ItemId = item.Id }
                })),

            new DefinitionCase(
                "contains-placement/nested-anonymous-member",
                conventions => Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, object>
                {
                    Map = docs => from doc in docs
                                  select new
                                  {
                                      doc.Id,
                                      Nested = new
                                      {
                                          HasMatch = MemoryExtensions.Contains(doc.IntValues, 42)
                                      }
                                  }
                })),

            new DefinitionCase(
                "contains-placement/member-init",
                conventions => Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, StructuralProjection>
                {
                    Map = docs => from doc in docs
                                  select new StructuralProjection
                                  {
                                      Id = doc.Id,
                                      HasMatch = MemoryExtensions.Contains(doc.IntValues, 42)
                                  }
                })),

            new DefinitionCase(
                "contains-placement/conditional-test",
                conventions => Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, object>
                {
                    Map = docs => from doc in docs
                                  select new
                                  {
                                      doc.Id,
                                      Result = MemoryExtensions.Contains(doc.IntValues, 42) ? doc.Threshold : 0
                                  }
                })),

            new DefinitionCase(
                "contains-placement/conditional-branches",
                conventions => Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, object>
                {
                    Map = docs => from doc in docs
                                  select new
                                  {
                                      doc.Id,
                                      HasMatch = doc.Flag
                                          ? MemoryExtensions.Contains(doc.IntValues, 42)
                                          : MemoryExtensions.Contains(doc.OtherIntValues, 42)
                                  }
                })),

            new DefinitionCase(
                "contains-placement/coalesce-argument",
                conventions => Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, object>
                {
                    Map = docs => from doc in docs
                                  select new
                                  {
                                      doc.Id,
                                      HasMatch = MemoryExtensions.Contains(doc.IntValues ?? Array.Empty<int>(), 42)
                                  }
                })),

            new DefinitionCase(
                "contains-placement/new-array-element",
                conventions => Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, object>
                {
                    Map = docs => from doc in docs
                                  select new
                                  {
                                      doc.Id,
                                      Flags = new[]
                                      {
                                          MemoryExtensions.Contains(doc.IntValues, 42),
                                          doc.Flag
                                      }
                                  }
                })),

            new DefinitionCase(
                "contains-placement/unary-negation",
                conventions => Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, object>
                {
                    Map = docs => from doc in docs
                                  select new
                                  {
                                      doc.Id,
                                      IsMissing = !MemoryExtensions.Contains(doc.IntValues, 42)
                                  }
                })),

            new DefinitionCase(
                "contains-placement/method-argument",
                conventions => Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, object>
                {
                    Map = docs => from doc in docs
                                  select new
                                  {
                                      doc.Id,
                                      NumericFlag = Convert.ToInt32(MemoryExtensions.Contains(doc.IntValues, 42))
                                  }
                })),

            ProjectionCase("binary-precedence/and-also", doc => MemoryExtensions.Contains(doc.IntValues, 42) && doc.Flag),
            ProjectionCase("binary-precedence/or-else", doc => MemoryExtensions.Contains(doc.IntValues, 42) || doc.Flag),
            ProjectionCase("binary-precedence/boolean-and", doc => MemoryExtensions.Contains(doc.IntValues, 42) & doc.Flag),
            ProjectionCase("binary-precedence/boolean-or", doc => MemoryExtensions.Contains(doc.IntValues, 42) | doc.Flag),

            ProjectionCase(
                "lambda-placement/where-predicate",
                doc => doc.OtherIntValues.Where(value => MemoryExtensions.Contains(doc.IntValues, value)).Count()),

            ProjectionCase(
                "lambda-placement/select-selector",
                doc => doc.OtherIntValues.Select(value => MemoryExtensions.Contains(doc.IntValues, value)).Count(hasMatch => hasMatch)),

            ProjectionCase(
                "lambda-placement/any-predicate",
                doc => doc.OtherIntValues.Any(value => MemoryExtensions.Contains(doc.IntValues, value))),

            ProjectionCase(
                "lambda-placement/all-predicate",
                doc => doc.OtherIntValues.All(value => MemoryExtensions.Contains(doc.IntValues, value))),

            ProjectionCase(
                "lambda-placement/count-predicate",
                doc => doc.OtherIntValues.Count(value => MemoryExtensions.Contains(doc.IntValues, value))),

            ProjectionCase(
                "lambda-placement/order-by-key-selector",
                doc => doc.OtherIntValues.OrderBy(value => MemoryExtensions.Contains(doc.IntValues, value)).FirstOrDefault()),

            ProjectionCase(
                "lambda-placement/group-by-key-selector",
                doc => doc.OtherIntValues.GroupBy(value => MemoryExtensions.Contains(doc.IntValues, value)).Count()),

            ProjectionCase(
                "lambda-placement/join-key-selectors",
                doc => doc.IntValues.Join(
                    doc.OtherIntValues,
                    outer => MemoryExtensions.Contains(doc.IntValues, outer),
                    inner => MemoryExtensions.Contains(doc.OtherIntValues, inner),
                    (outer, inner) => outer).Count()),

            ProjectionCase(
                "lambda-placement/group-join-result-selector",
                doc => doc.IntValues.GroupJoin(
                    doc.OtherIntValues,
                    outer => outer,
                    inner => inner,
                    (outer, matches) => MemoryExtensions.Contains(doc.IntValues, outer) && matches.Any())
                    .Count(hasMatch => hasMatch)),

            ProjectionCase(
                "lambda-placement/select-many-result-selector",
                doc => doc.Items.SelectMany(
                    item => item.Values,
                    (item, value) => MemoryExtensions.Contains(item.Values, value))
                    .Count(hasMatch => hasMatch)),

            MapReduceCase(
                "map-reduce-placement/map-where",
                () => new IndexDefinitionBuilder<StructuralDoc, StructuralReduceResult>
                {
                    Map = docs => from doc in docs
                                  where MemoryExtensions.Contains(doc.IntValues, 42)
                                  select new StructuralReduceResult
                                  {
                                      Key = doc.Id,
                                      Count = 1,
                                      HasMatch = true
                                  },
                    Reduce = results => from result in results
                                        group result by result.Key into g
                                        select new StructuralReduceResult
                                        {
                                            Key = g.Key,
                                            Count = g.Sum(x => x.Count),
                                            HasMatch = g.Any(x => x.HasMatch)
                                        }
                }),

            MapReduceCase(
                "map-reduce-placement/map-let",
                () => new IndexDefinitionBuilder<StructuralDoc, StructuralReduceResult>
                {
                    Map = docs => from doc in docs
                                  let hasMatch = MemoryExtensions.Contains(doc.IntValues, 42)
                                  select new StructuralReduceResult
                                  {
                                      Key = doc.Id,
                                      Count = 1,
                                      HasMatch = hasMatch
                                  },
                    Reduce = results => from result in results
                                        group result by result.Key into g
                                        select new StructuralReduceResult
                                        {
                                            Key = g.Key,
                                            Count = g.Sum(x => x.Count),
                                            HasMatch = g.Any(x => x.HasMatch)
                                        }
                }),

            MapReduceCase(
                "map-reduce-placement/reduce-where",
                () => ReduceBuilder(results => from result in results
                                                            group result by result.Key into g
                                                            where MemoryExtensions.Contains(new[] { "keep", "other" }, g.Key)
                                                            select new StructuralReduceResult
                                                            {
                                                                Key = g.Key,
                                                                Count = g.Sum(x => x.Count),
                                                                HasMatch = true
                                                            })),

            MapReduceCase(
                "map-reduce-placement/reduce-let",
                () => ReduceBuilder(results => from result in results
                                                            group result by result.Key into g
                                                            let hasMatch = MemoryExtensions.Contains(new[] { "keep", "other" }, g.Key)
                                                            select new StructuralReduceResult
                                                            {
                                                                Key = g.Key,
                                                                Count = g.Sum(x => x.Count),
                                                                HasMatch = hasMatch
                                                            })),

            MapReduceCase(
                "map-reduce-placement/reduce-projection",
                () => ReduceBuilder(results => from result in results
                                                            group result by result.Key into g
                                                            select new StructuralReduceResult
                                                            {
                                                                Key = g.Key,
                                                                Count = g.Sum(x => x.Count),
                                                                HasMatch = MemoryExtensions.Contains(new[] { "keep", "other" }, g.Key)
                                                            })),

            MapReduceCase(
                "map-reduce-placement/reduce-predicate",
                () => ReduceBuilder(results => from result in results
                                                            group result by result.Key into g
                                                            select new StructuralReduceResult
                                                            {
                                                                Key = g.Key,
                                                                Count = g.Sum(x => x.Count),
                                                                HasMatch = g.Any(x => MemoryExtensions.Contains(new[] { 1, 2, 3 }, x.Count))
                                                            })),

            ProjectionCase(
                "enumerable-rewrite/instance-contains-projection",
                doc => doc.IntValues.Contains(42)),

            new DefinitionCase(
                "enumerable-rewrite/instance-contains-query-where",
                conventions => Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, object>
                {
                    Map = docs => from doc in docs
                                  where doc.IntValues.Contains(42)
                                  select new { doc.Id }
                })),

            ProjectionCase(
                "enumerable-rewrite/static-contains-projection",
                doc => Enumerable.Contains(doc.IntValues, 42)),

            ProjectionCase(
                "enumerable-rewrite/sequence-equal-projection",
                doc => doc.IntValues.SequenceEqual(doc.OtherIntValues)),

            ProjectionCase(
                "enumerable-rewrite/except-nested-projection",
                doc => doc.IntValues.Except(doc.OtherIntValues).Count()),

            new DefinitionCase(
                "enumerable-rewrite/intersect-query-where",
                conventions => Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, object>
                {
                    Map = docs => from doc in docs
                                  where doc.IntValues.Intersect(doc.OtherIntValues).Any()
                                  select new { doc.Id }
                })),

            ProjectionCase(
                "enumerable-rewrite/union-inside-selector",
                doc => doc.IntValues
                    .Select(value => doc.IntValues.Union(doc.OtherIntValues).Contains(value))
                    .Count(hasMatch => hasMatch)),

            ProjectionCase(
                "enumerable-rewrite/join-projection",
                doc => doc.IntValues.Join(
                    doc.OtherIntValues,
                    outer => outer,
                    inner => inner,
                    (outer, inner) => outer).Count()),

            ProjectionCase(
                "enumerable-rewrite/of-type-primitive",
                doc => doc.IntValues.Cast<object>().OfType<int>().Count()),

            new DefinitionCase(
                "enumerable-rewrite/contains-comparer",
                conventions => Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, object>
                {
                    Map = docs => from doc in docs
                                  select new
                                  {
                                      doc.Id,
                                      HasMatch = doc.Tags.Contains("A", StringComparer.OrdinalIgnoreCase)
                                  }
                })),

            ProjectionCase(
                "typed-lambda/double-where-predicate",
                doc => doc.DoubleValues.Where(value => value > doc.DoubleThreshold).Sum()),
            ProjectionCase(
                "typed-lambda/double-select-selector",
                doc => doc.DoubleValues.Select(value => value * doc.DoubleThreshold).Sum()),
            ProjectionCase(
                "typed-lambda/decimal-where-predicate",
                doc => doc.DecimalValues.Where(value => value > doc.DecimalThreshold).Sum()),
            ProjectionCase(
                "typed-lambda/decimal-select-selector",
                doc => doc.DecimalValues.Select(value => value * doc.DecimalThreshold).Sum()),
            ProjectionCase(
                "typed-lambda/float-where-predicate",
                doc => doc.FloatValues.Where(value => value > doc.FloatThreshold).Sum()),
            ProjectionCase(
                "typed-lambda/float-select-selector",
                doc => doc.FloatValues.Select(value => value * doc.FloatThreshold).Sum()),

            ProjectionCase(
                "char-conversion/where-predicate",
                doc => doc.CharValues.Where(value => value == doc.Character).Count()),
            ProjectionCase(
                "char-conversion/select-selector",
                doc => doc.CharValues.Select(value => char.ToUpper(value)).FirstOrDefault()),
            ProjectionCase(
                "char-conversion/contains-argument",
                doc => doc.CharValues.Contains(doc.Character)),
            ProjectionCase(
                "char-conversion/convert-method-argument",
                doc => doc.CharValues.Select(value => Convert.ToInt32(value)).Sum())
        ];
    }

    private static DefinitionCase ProjectionCase<TResult>(
        string caseId,
        Expression<Func<StructuralDoc, TResult>> selector)
    {
        return new DefinitionCase(caseId, conventions => ProjectionDefinition(conventions, selector));
    }

    private static DefinitionCase MapReduceCase(
        string caseId,
        Func<IndexDefinitionBuilder<StructuralDoc, StructuralReduceResult>> builder)
    {
        return new DefinitionCase(caseId, conventions => Definition(conventions, builder()));
    }

    private static IndexDefinitionBuilder<StructuralDoc, StructuralReduceResult> ReduceBuilder(
        Expression<Func<IEnumerable<StructuralReduceResult>, System.Collections.IEnumerable>> reduce)
    {
        return new IndexDefinitionBuilder<StructuralDoc, StructuralReduceResult>
        {
            Map = docs => from doc in docs
                          select new StructuralReduceResult
                          {
                              Key = doc.Id,
                              Count = 1,
                              HasMatch = false
                          },
            Reduce = reduce
        };
    }

    private static IndexDefinition Definition<TDocument, TResult>(
        DocumentConventions conventions,
        IndexDefinitionBuilder<TDocument, TResult> builder)
    {
        return builder.ToIndexDefinition(conventions);
    }

    private static IndexDefinition ProjectionDefinition<TResult>(
        DocumentConventions conventions,
        Expression<Func<StructuralDoc, TResult>> selector)
    {
        var docsParameter = Expression.Parameter(typeof(IEnumerable<StructuralDoc>), "docs");
        var docParameter = Expression.Parameter(typeof(StructuralDoc), "doc");
        var selectorBody = new ReplaceParameterVisitor(selector.Parameters[0], docParameter).Visit(selector.Body);
        var resultType = typeof(StructuralValueProjection<TResult>);
        var resultBody = Expression.MemberInit(
            Expression.New(resultType),
            Expression.Bind(resultType.GetProperty(nameof(StructuralValueProjection<TResult>.Id)),
                Expression.Property(docParameter, nameof(StructuralDoc.Id))),
            Expression.Bind(resultType.GetProperty(nameof(StructuralValueProjection<TResult>.Result)), selectorBody));
        var itemSelector = Expression.Lambda<Func<StructuralDoc, StructuralValueProjection<TResult>>>(resultBody, docParameter);
        var selectCall = Expression.Call(
            typeof(Enumerable),
            nameof(Enumerable.Select),
            new[] { typeof(StructuralDoc), resultType },
            docsParameter,
            itemSelector);
        var map = Expression.Lambda<Func<IEnumerable<StructuralDoc>, System.Collections.IEnumerable>>(
            selectCall,
            docsParameter);

        return Definition(conventions, new IndexDefinitionBuilder<StructuralDoc, StructuralValueProjection<TResult>>
        {
            Map = map
        });
    }

    internal sealed class StructuralDoc
    {
        public string Id { get; set; }
        public int[] IntValues { get; set; }
        public int[] OtherIntValues { get; set; }
        public double[] DoubleValues { get; set; }
        public decimal[] DecimalValues { get; set; }
        public float[] FloatValues { get; set; }
        public char[] CharValues { get; set; }
        public string[] Tags { get; set; }
        public StructuralItem[] Items { get; set; }
        public int Threshold { get; set; }
        public bool Flag { get; set; }
        public char Character { get; set; }
        public double DoubleThreshold { get; set; }
        public decimal DecimalThreshold { get; set; }
        public float FloatThreshold { get; set; }
    }

    internal sealed class StructuralItem
    {
        public string Id { get; set; }
        public int[] Values { get; set; }
    }

    internal sealed class StructuralProjection
    {
        public string Id { get; set; }
        public bool HasMatch { get; set; }
    }

    internal sealed class StructuralValueProjection<TResult>
    {
        public string Id { get; set; }
        public TResult Result { get; set; }
    }

    internal sealed class StructuralReduceResult
    {
        public string Key { get; set; }
        public int Count { get; set; }
        public bool HasMatch { get; set; }
    }

    private sealed class ReplaceParameterVisitor : ExpressionVisitor
    {
        private readonly ParameterExpression _source;
        private readonly ParameterExpression _target;

        public ReplaceParameterVisitor(ParameterExpression source, ParameterExpression target)
        {
            _source = source;
            _target = target;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            return node == _source ? _target : base.VisitParameter(node);
        }
    }
}

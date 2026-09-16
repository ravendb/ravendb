using System;
using System.Collections.Generic;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Voron.Data.RoaringBitmaps;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Voron.Benchmark.Corax
{
    public class CoraxQueryEvaluator
    {
        private readonly IndexSearcher _searcher;

        public CoraxQueryEvaluator(IndexSearcher searcher)
        {
            _searcher = searcher;
        }

        public IQueryMatch Search(string q)
        {
            var parser = new QueryParser();
            parser.Init(q);
            var query = parser.Parse();
            return Search(query.Where);
        }

        public IQueryMatch Search(QueryExpression where)
        {
            return Evaluate(@where);
        }

        private IQueryMatch Evaluate(QueryExpression where)
        {
            switch (@where)
            {
                case TrueExpression _:
                case null:
                    return null; // all docs here
                case InExpression ie:
                    return (ie.Source, ie.Values) switch
                    {
                        (FieldExpression f, List<QueryExpression> list) => EvaluateInExpression(f, list),
                        _ => throw new NotSupportedException()
                    };
                case BinaryExpression be:
                    return (be.Operator, be.Left, be.Right) switch
                    {
                        (OperatorType.Equal, FieldExpression f, ValueExpression v) => _searcher.TermQuery(f.FieldValue, v.Token.Value),
                        (OperatorType.And, var l, var r) => And(Evaluate(l), Evaluate(r)),
                        (OperatorType.Or, var l, var r) => Or(Evaluate(l), Evaluate(r)),
                        _ => throw new NotSupportedException()
                    };
                default:
                    return null;
            }
        }

        // IndexSearcher has no And/Or primitives — compose sub-matches via bitmap primitives.
        // A null sub-match means "all documents" (e.g. a TrueExpression), so it acts as the
        // identity for AND and the absorbing element for OR.
        private IQueryMatch And(IQueryMatch left, IQueryMatch right)
        {
            if (left == null) return right;
            if (right == null) return left;

            var bitmap = new BitmapMatch(_searcher.Allocator);
            RoaringBitmap tempData = new(_searcher.Allocator);
            try
            {
                QueryPrimitives.OrWithMatch(left, ref bitmap.BitmapState);
                QueryPrimitives.AndWithMatch(right, ref bitmap.BitmapState, ref tempData);
            }
            finally
            {
                tempData.Dispose();
            }
            return bitmap;
        }

        private IQueryMatch Or(IQueryMatch left, IQueryMatch right)
        {
            if (left == null || right == null) return null;

            var bitmap = new BitmapMatch(_searcher.Allocator);
            QueryPrimitives.OrWithMatch(left, ref bitmap.BitmapState);
            QueryPrimitives.OrWithMatch(right, ref bitmap.BitmapState);
            return bitmap;
        }

        private IQueryMatch EvaluateInExpression(FieldExpression f, List<QueryExpression> list)
        {
            var values = new List<string>();
            foreach (ValueExpression v in list)
                values.Add(v.Token.Value);

            return _searcher.InQuery(f.FieldValue, values);
        }
    }

    public class QueryDefinition
    {
        /// <summary>
        /// This is the means by which the outside world refers to this query
        /// </summary>
        public string Name { get; private set; }

        public Query Query { get; private set; }

        public QueryDefinition(string name, Query query)
        {
            Name = name;
            Query = query;
        }
    }
}

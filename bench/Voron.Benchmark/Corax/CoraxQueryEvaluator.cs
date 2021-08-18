using System;
using System.Collections.Generic;
using Corax;
using Corax.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;

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
                        (OperatorType.And, QueryExpression q1, QueryExpression q2) => _searcher.And(Evaluate(q1), Evaluate(q2)),
                        (OperatorType.Or, QueryExpression q1, QueryExpression q2) => _searcher.Or(Evaluate(q1), Evaluate(q2)),
                        _ => throw new NotSupportedException()
                    };
                default:
                    return null;
            }
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

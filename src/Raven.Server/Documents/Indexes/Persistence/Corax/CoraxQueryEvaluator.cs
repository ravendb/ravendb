using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Queries;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxQueryEvaluator
    {
        private readonly IndexSearcher _searcher;

        public CoraxQueryEvaluator(IndexSearcher searcher)
        {
            _searcher = searcher;
        }

        public IQueryMatch Search(Query query)
        {
            return Search(query.Where);
        }

        public IQueryMatch Search(QueryExpression where)
        {
            return Evaluate(@where);
        }

        //When we are using aliases we need to escape it.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string GetField(FieldExpression f)
        {
            return f.FieldValue.Length != 0 ? f.FieldValue : f.FieldValueWithoutAlias;
        }

        private IQueryMatch Evaluate(QueryExpression where)
        {
            switch (@where)
            {
                case MethodExpression me:
                    var exprssionType = QueryMethod.GetMethodType(me.Name.Value);
                    switch (exprssionType)
                    {
                        case MethodType.StartsWith:
                            return _searcher.StartWithQuery(GetField((FieldExpression)me.Arguments[0]), ((ValueExpression)me.Arguments[1]).Token.Value);
                        default:
                            return null;
                    }
                case TrueExpression _:
                case null:
                    return null;
                case InExpression ie:
                    return (ie.Source, ie.Values) switch
                    {
                        (FieldExpression f, List<QueryExpression> list) => EvaluateInExpression(f, list),
                        _ => throw new NotSupportedException()
                    };
                case BinaryExpression be:
                    return (be.Operator, be.Left, be.Right) switch
                    {
                        (OperatorType.Equal, FieldExpression f, ValueExpression v) => _searcher.TermQuery(GetField(f), v.Token.Value),
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
}

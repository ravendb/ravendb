using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Queries;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxQueryEvaluator
    {
        private readonly IndexSearcher _searcher;
        private IndexQueryServerSide _query;

        public CoraxQueryEvaluator(IndexSearcher searcher)
        {
            _searcher = searcher;
        }

        public IQueryMatch Search(Query query)
        {
            return Search(query.Where);
        }

        public IQueryMatch Search(IndexQueryServerSide query, FieldsToFetch fieldsToFetch)
        {
            _query = query;
            IQueryMatch result = null;
            if (query.Metadata.Query.Where is null)
                throw new NotImplementedException("Corax all docs.");
            result = Evaluate(query.Metadata.Query.Where);
            if (query.Metadata.Query.OrderBy != null)
                result = OrderByEvaluate(result, fieldsToFetch, query.Metadata.Query.OrderBy);
            return result;
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

        private string GetFieldValue(ValueExpression f)
        {
            object value = f.Token.Value;
            if (f.Value == ValueTokenType.Parameter)
                 if (_query.QueryParameters.TryGet(f.Token.Value, out value) == false)
                     throw new InvalidDataException($"Cannot find {f.Token.Value} parameter. Please check your query.");
            return value.ToString();
        }

        private IQueryMatch Evaluate(QueryExpression where)
        {
            switch (@where)
            {
                case MethodExpression me:
                    var expressionType = QueryMethod.GetMethodType(me.Name.Value);
                    switch (expressionType)
                    {
                        case MethodType.StartsWith:
                            return _searcher.StartWithQuery(GetField((FieldExpression)me.Arguments[0]), ((ValueExpression)me.Arguments[1]).Token.Value);
                        default:
                            return null;
                    }
                case null:
                    return _searcher.AllEntries();
                case InExpression ie:
                    return (ie.Source, ie.Values) switch
                    {
                        (FieldExpression f, List<QueryExpression> list) => EvaluateInExpression(f, list),
                        _ => throw new NotSupportedException()
                    };
                case BinaryExpression be:
                    return (be.Operator, be.Left, be.Right) switch
                    {
                        (OperatorType.Equal, FieldExpression f, ValueExpression v) => _searcher.TermQuery(GetField(f), GetFieldValue(v)),
                        (OperatorType.And, QueryExpression q1, QueryExpression q2) => _searcher.And(Evaluate(q1), Evaluate(q2)),
                        (OperatorType.Or, QueryExpression q1, QueryExpression q2) => _searcher.Or(Evaluate(q1), Evaluate(q2)),
                        _ => throw new NotSupportedException($"Method {be} is not supported.")
                    };
                default:
                    throw new NotSupportedException();
            }
        }

        private IQueryMatch EvaluateInExpression(FieldExpression f, List<QueryExpression> list)
        {
            var values = new List<string>();
            foreach (ValueExpression v in list)
                values.Add(v.Token.Value);

            return _searcher.InQuery(f.FieldValue, values);
        }

        private MatchCompareFieldType OrderTypeFieldConventer(OrderByFieldType original) =>
            original switch
            {
                OrderByFieldType.Double => MatchCompareFieldType.Floating,
                OrderByFieldType.Long => MatchCompareFieldType.Integer,
                OrderByFieldType.AlphaNumeric => MatchCompareFieldType.Integer,
                _ => MatchCompareFieldType.Sequence
            };
        
        private IQueryMatch OrderByEvaluate(IQueryMatch result, FieldsToFetch fieldsToFetch, List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orderList)
        {
            foreach (var order in orderList)
            {
                if (order.Expression is not FieldExpression fe) continue;
                var fieldName = GetField(fe);
                var id = fieldsToFetch.IndexFields[GetField(fe)].Id;
                if (order.Ascending == false)
                {
                    result = _searcher.OrderByDescending(result, id, OrderTypeFieldConventer(order.FieldType));
                }
                else
                {
                    result = _searcher.OrderByAscending(result, id, OrderTypeFieldConventer(order.FieldType));
                }
            }
            
            return result;
        }
    }
}

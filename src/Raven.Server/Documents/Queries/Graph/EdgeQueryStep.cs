using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Utils;
using Sparrow.Json;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public class EdgeQueryStep : IQueryStep
    {

        public EdgeQueryStep(IQueryStep left, IQueryStep right, WithEdgesExpression edgesExpression, MatchPath edgePath, BlittableJsonReaderObject queryParameters)
        {
            _left = left;
            _right = right;
            _edgePath = edgePath;
            _queryParameters = queryParameters;
            _edgesExpression = edgesExpression;
        }

        public async ValueTask<IEnumerable<Match>> Execute(Dictionary<IQueryStep, IEnumerable<Match>> matches)
        {
            var leftResult = await _left.Execute(matches);
            var rightResult = await _right.Execute(matches);

            var edgeAlias = _edgePath.Alias;
            var edge = _edgesExpression;
            edge.EdgeAlias = edgeAlias;
            var res = new List<Match>();
            foreach (var left in leftResult)
            {
                _included.Clear();
                var leftDoc = left.GetFirstResult().Data;
                if (!leftDoc.TryGetMember(edge.Path.FieldValue, out var field))
                    continue;

                switch (field)
                {
                    case LazyStringValue _:
                        //hasResults 
                        var leftDocInclude = new EdgeIncludeOp(this);
                        IncludeUtil.GetDocIdFromInclude(leftDoc, edge.Path.FieldValue, leftDocInclude);
                        if (_included.Count > 0)
                        {
                            foreach (var kvp in _included)
                            {
                                if (kvp.Key == null)
                                    continue;

                                foreach (var right in rightResult)
                                {
                                    foreach (var alias in right.Aliases)
                                    {
                                        var doc = right.GetSingleDocumentResult(alias);
                                        if (doc.Id != kvp.Key)
                                            continue;

                                        var clone = new Match(left);
                                        clone.Merge(right);
                                        //clone -> result row
                                        res.Add(clone);
                                        break;
                                    }
                                }
                            }
                        }

                        break;
                    case BlittableJsonReaderArray array:
                        break;
                    case BlittableJsonReaderObject json:
                        if (edge.Where.IsMatchedBy(json, _queryParameters))
                        {
                            //hasResults 
                            var includeFields = new EdgeIncludeOp();
                            IncludeUtil.GetDocIdFromInclude(json, edge.Path.FieldValue, includeFields);
                            if (_included.Count > 0)
                            {
                                foreach (var kvp in _included)
                                {
                                    if (kvp.Key == null)
                                        continue;

                                    foreach (var right in rightResult)
                                    {
                                        foreach (var alias in right.Aliases)
                                        {
                                            var doc = right.GetSingleDocumentResult(alias);
                                            if (doc.Id != kvp.Key)
                                                continue;

                                            var clone = new Match(left);
                                            clone.Merge(right);
                                            //clone -> result row
                                            res.Add(clone);
                                            break;
                                        }
                                    }
                                }
                            }

                        }
                        break;
                }
            }

            return res;
        }

        private IQueryStep _left;
        private IQueryStep _right;
        private MatchPath _edgePath;
        private readonly BlittableJsonReaderObject _queryParameters;
        private WithEdgesExpression _edgesExpression;
        private Dictionary<string, BlittableJsonReaderObject> _included = new Dictionary<string, BlittableJsonReaderObject>();

        public IEnumerable<IQueryStep> Dependencies => GetDependencies();

        private IEnumerable<IQueryStep> GetDependencies()
        {
            yield return _left;
            yield return _right;
        }

        private struct EdgeIncludeOp : IncludeUtil.IIncludeOp
        {
            private EdgeQueryStep _parent;

            public EdgeIncludeOp(EdgeQueryStep parent)
            {
                _parent = parent;
            }

            public void Include(BlittableJsonReaderObject parent, string id)
            {
                _parent._included.Add(id,parent);
            }
        }
    }
}

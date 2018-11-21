using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.Queries.Graph
{
    public class CollectionDestinationQueryStep : IGraphQueryStep
    {
        private StringSegment _alias;
        private HashSet<string> _aliases;
        DocumentsOperationContext _context;
        DocumentsStorage _documentStorage;
        private List<GraphQueryRunner.Match> _temp = new List<GraphQueryRunner.Match>();
        private string _colectionName;

        public CollectionDestinationQueryStep(StringSegment alias, DocumentsOperationContext documentsContext, DocumentsStorage documentStorage, string colectionName)
        {
            _colectionName = colectionName;
            _alias = alias;
            _aliases = new HashSet<string> { alias };
            _context = documentsContext;
            _documentStorage = documentStorage;
        }

        public bool IsEmpty()
        {
            if (_colectionName == string.Empty)
                return _documentStorage.GetNumberOfDocuments() == 0;
            return _documentStorage.GetCollection(_colectionName, _context).Count == 0;
        }

        public IGraphQueryStep Clone()
        {
            return new CollectionDestinationQueryStep(_alias, _context, _documentStorage, _colectionName);
        }

        public void Analyze(GraphQueryRunner.Match match, GraphQueryRunner.GraphDebugInfo graphDebugInfo)
        {
            var result = match.GetResult(_alias);
            if (result == null)
                return;

            if (result is Document d && d.Id != null)
            {
                graphDebugInfo.AddNode(d.Id.ToString(), d);
            }
            else
            {
                graphDebugInfo.AddNode(null, result);
            }
        }

        public HashSet<string> GetAllAliases()
        {
            return _aliases;
        }

        public bool GetNext(out GraphQueryRunner.Match match)
        {
            throw new NotImplementedException("Query step of type CollectionDestinationQueryStep should not be invoking 'GetNext' method this is an indication of a bug.");
        }

        public string GetOutputAlias()
        {
            return _alias;
        }

        public ValueTask Initialize()
        {
            return default;
        }

        public List<GraphQueryRunner.Match> GetById(string id)
        {
            _temp.Clear();

            var document = _documentStorage.Get(_context, id);
            var match = new GraphQueryRunner.Match();
            if (_colectionName == string.Empty /* in the case of alias like '_'*/ ||
                document != null && document.TryGetMetadata(out var metadata)
                && metadata.TryGetWithoutThrowingOnError(Constants.Documents.Metadata.Collection, out string cn)
                && cn == _colectionName)
            {
                match.Set(_alias, document);
                _temp.Add(match);
            }
            return _temp;
        }

        public ISingleGraphStep GetSingleGraphStepExecution()
        {
            throw new NotSupportedException();
        }
    }
}

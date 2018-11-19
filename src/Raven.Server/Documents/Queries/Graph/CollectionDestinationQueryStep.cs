using System;
using System.Collections.Generic;
using System.Threading.Tasks;
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

        public CollectionDestinationQueryStep(Sparrow.StringSegment alias, DocumentsOperationContext documentsContext, DocumentsStorage documentStorage)
        {
            _alias = alias;
            _aliases = new HashSet<string> { alias };
            _context = documentsContext;
            _documentStorage = documentStorage;
        }

        public IGraphQueryStep Clone()
        {
            return new CollectionDestinationQueryStep(_alias, _context, _documentStorage);
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
            if (document != null)
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

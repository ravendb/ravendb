using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Queries.Graph
{
    public class CollectionDestinationQueryStep : IGraphQueryStep
    {
        private StringSegment _alias;
        private HashSet<string> _aliases;
        DocumentsOperationContext _context;
        DocumentsStorage _documentStorage;
        private List<GraphQueryRunner.Match> _temp = new List<GraphQueryRunner.Match>();
        public readonly string CollectionName;
        private OperationCancelToken _token;

        public CollectionDestinationQueryStep(StringSegment alias, DocumentsOperationContext documentsContext, DocumentsStorage documentStorage, string collectionName, OperationCancelToken token)
        {
            CollectionName = collectionName;
            _alias = alias;
            _aliases = new HashSet<string> { alias.Value };
            _context = documentsContext;
            _documentStorage = documentStorage;
            _token = token;
        }

        public bool IsEmpty()
        {
            if (CollectionName == string.Empty)
                return _documentStorage.GetNumberOfDocuments() == 0;
            return _documentStorage.GetCollection(CollectionName, _context).Count == 0;
        }

        public bool CollectIntermediateResults { get; set; } 

        public List<GraphQueryRunner.Match> IntermediateResults { get; } = new List<GraphQueryRunner.Match>();

        public IGraphQueryStep Clone()
        {
            return new CollectionDestinationQueryStep(_alias, _context, _documentStorage, CollectionName, _token)
            {
                CollectIntermediateResults = CollectIntermediateResults
            };
            ;
        }

        public void Analyze(GraphQueryRunner.Match match, GraphQueryRunner.GraphDebugInfo graphDebugInfo)
        {
            var result = match.GetResult(_alias.Value);
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
            return _alias.Value;
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
            if (CollectionName == string.Empty /* in the case of alias like '_'*/ ||
                document != null && document.TryGetMetadata(out var metadata)
                && metadata.TryGetWithoutThrowingOnError(Constants.Documents.Metadata.Collection, out string cn)
                && cn == CollectionName)
            {
                match.Set(_alias, document);
                _temp.Add(match);
            }

            if (CollectIntermediateResults)
            {
                IntermediateResults.AddRange(_temp);
            }
            return _temp;
        }

        public ISingleGraphStep GetSingleGraphStepExecution()
        {
            throw new NotSupportedException();
        }
    }
}

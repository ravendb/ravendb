using Esprima.Ast;
using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Queries
{
    public class HasLoadIncludeCounterOrCmpXcngVisitor : EsprimaVisitor
    {
        private readonly QueryMetadata _queryMetadata;

        //todo aviv : change class name 

        public HasLoadIncludeCounterOrCmpXcngVisitor(QueryMetadata queryMetadata)
        {
            _queryMetadata = queryMetadata;
        }

        public override void VisitCallExpression(CallExpression callExpression)
        {
            if (_queryMetadata.HasIncludeOrLoad && _queryMetadata.HasCounterSelect && 
                _queryMetadata.HasCmpXchgSelect && _queryMetadata.HasTimeSeriesSelect)
                return;

            if (callExpression.Callee is Identifier id )
            {
                switch (id.Name)
                {
                    case "load":
                    case "include":
                    case "loadPath":
                        _queryMetadata.HasIncludeOrLoad = true;
                        break;
                    case "counter":
                    case "counterRaw":
                        _queryMetadata.HasCounterSelect = true;
                        break;
                    case "cmpxchg":
                        _queryMetadata.HasCmpXchgSelect = true;
                        break;
                    case "timeseries":
                        _queryMetadata.HasTimeSeriesSelect = true;
                        break;
                }
            }

            base.VisitCallExpression(callExpression);
        }
    }
}

using Esprima.Ast;
using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Queries
{
    public class HasSpecialMethodVisitor : EsprimaVisitor
    {
        private readonly QueryMetadata _queryMetadata;

        public HasSpecialMethodVisitor(QueryMetadata queryMetadata)
        {
            _queryMetadata = queryMetadata;
        }

        public override void VisitCallExpression(CallExpression callExpression)
        {
            if (_queryMetadata.HasIncludeOrLoad &&
                _queryMetadata.HasCounterSelect &&
                _queryMetadata.HasCmpXchgSelect &&
                _queryMetadata.HasTimeSeriesSelect &&
                _queryMetadata.HasCmpXchgIncludes)
                return;

            if (callExpression.Callee is Identifier id)
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

            if (callExpression.Callee is StaticMemberExpression @static)
            {
                var staticId = @static.Object as Identifier;
                var staticCallId = @static.Property as Identifier;
                if (staticId != null && staticCallId != null)
                {
                    switch (staticId.Name)
                    {
                        case "includes":
                            switch (staticCallId.Name)
                            {
                                case "cmpxchg":
                                    _queryMetadata.HasCmpXchgIncludes = true;
                                    break;
                            }
                            break;
                    }
                }
            }

            base.VisitCallExpression(callExpression);
        }
    }
}

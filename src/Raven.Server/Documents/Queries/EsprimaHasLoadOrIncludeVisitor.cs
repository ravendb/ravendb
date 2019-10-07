using Esprima.Ast;
using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Queries
{
    public class EsprimaHasLoadOrIncludeOrCounterVisitor : EsprimaVisitor
    {
        private readonly QueryMetadata _queryMetadata;

        public EsprimaHasLoadOrIncludeOrCounterVisitor(QueryMetadata queryMetadata)
        {
            _queryMetadata = queryMetadata;
        }

        public override void VisitCallExpression(CallExpression callExpression)
        {
            if (_queryMetadata.HasIncludeOrLoad && _queryMetadata.HasCounterSelect)
                return;

            if (callExpression.Callee is Identifier id )
            {
                if (id.Name.Equals("load") || id.Name.Equals("include") || id.Name.Equals("loadPath"))
                {
                    _queryMetadata.HasIncludeOrLoad = true;
                }
                else if (id.Name.Equals("counter") || id.Name.Equals("counterRaw"))
                {
                    _queryMetadata.HasCounterSelect = true;
                }
            }

            base.VisitCallExpression(callExpression);
        }
    }
}

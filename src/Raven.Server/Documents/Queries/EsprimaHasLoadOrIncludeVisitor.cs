using Esprima.Ast;
using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Queries
{
    public class EsprimaHasLoadOrIncludeVisitor : EsprimaVisitor
    {
        private readonly QueryMetadata _queryMetadata;

        public EsprimaHasLoadOrIncludeVisitor(QueryMetadata queryMetadata)
        {
            _queryMetadata = queryMetadata;
        }

        public override void VisitCallExpression(CallExpression callExpression)
        {
            if (_queryMetadata.HasIncludeOrLoad)
                return;

            if (callExpression.Callee is Identifier id &&
                (id.Name.Equals("load") || id.Name.Equals("include") || id.Name.Equals("loadPath")))
            {
                _queryMetadata.HasIncludeOrLoad = true;
                return;
            }

            base.VisitCallExpression(callExpression);
        }
    }
}

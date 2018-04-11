using System;
using System.Collections.Generic;
using System.Text;
using Esprima.Ast;

namespace Raven.Server.Documents.Indexes.Static
{
    public class EsprimaReferencedCollectionVisitor:EsprimaVisitor
    {
        public readonly HashSet<CollectionName> ReferencedCollection = new HashSet<CollectionName>();
        public override void VisitCallExpression(CallExpression callExpression)
        {
            if (callExpression.Callee is Identifier id 
                && id.Name.Equals("load") )
            {
                var collection = callExpression.Arguments[1];
                if (collection is Literal l && l.Value is string s)
                {
                    ReferencedCollection.Add(new CollectionName(s));
                }
            }
            base.VisitCallExpression(callExpression);
        }
    }
}

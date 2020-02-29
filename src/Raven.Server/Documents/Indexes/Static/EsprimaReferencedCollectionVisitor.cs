using System;
using System.Collections.Generic;
using Esprima.Ast;

namespace Raven.Server.Documents.Indexes.Static
{
    public class EsprimaReferencedCollectionVisitor : EsprimaVisitor
    {
        public readonly HashSet<CollectionName> ReferencedCollection = new HashSet<CollectionName>();

        public bool HasCompareExchangeReferences { get; private set; }

        public override void VisitCallExpression(CallExpression callExpression)
        {
            if (callExpression.Callee is Identifier id)
            {
                if (id.Name.Equals("load"))
                {
                    if (callExpression.Arguments.Count != 2)
                    {
                        throw new ArgumentException("load method is expecting two arguments, the first should be the document and the second should be the collection. e.g. load(u.Product,'Products') but was invoked with " +
                            $"{callExpression.Arguments.Count} arguments.");
                    }
                    var collection = callExpression.Arguments[1];
                    if (collection is Literal l && l.Value is string s)
                    {
                        ReferencedCollection.Add(new CollectionName(s));
                    }
                }
                else if (id.Name.Equals("cmpxchg"))
                    HasCompareExchangeReferences = true;
            }

            base.VisitCallExpression(callExpression);
        }
    }
}

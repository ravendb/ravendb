using System;
using System.Collections.Generic;
using Acornima.Ast;

namespace Raven.Server.Documents.Indexes.Static
{
    public sealed class AcornimaReferencedCollectionVisitor : AcornimaVisitor
    {
        public readonly HashSet<CollectionName> ReferencedCollection = new HashSet<CollectionName>();

        public bool HasCompareExchangeReferences { get; private set; }

        public override void VisitCallExpression(CallExpression callExpression)
        {
            if (TryGetIdentifier(callExpression, out var id, out bool noTracking))
            {
                switch (id.Name)
                {
                    case JavaScriptIndex.Load:
                        {
                            if (callExpression.Arguments.Count != 2)
                            {
                                throw new ArgumentException("load method is expecting two arguments, the first should be the document and the second should be the collection. e.g. load(u.Product,'Products') but was invoked with " +
                                                            $"{callExpression.Arguments.Count} arguments.");
                            }

                            if (noTracking == false)
                            {
                                var collection = callExpression.Arguments[1];
                                if (collection is Literal { Value: string s })
                                {
                                    ReferencedCollection.Add(new CollectionName(s));
                                }
                            }
                        }
                        break;

                    case JavaScriptIndex.CmpXchg:
                        HasCompareExchangeReferences = true;
                        break;
                }
            }

            base.VisitCallExpression(callExpression);

            static bool TryGetIdentifier(CallExpression callExpression, out Identifier identifier, out bool noTracking)
            {
                switch (callExpression.Callee)
                {
                    case Identifier i:
                        identifier = i;
                        noTracking = false;

                        return true;
                    case MemberExpression sme:
                        {
                            if (sme.Object is Identifier { Name: JavaScriptIndex.NoTracking })
                            {
                                noTracking = true;

                                if (sme.Property is Identifier propertyIdentifier)
                                {
                                    identifier = propertyIdentifier;
                                    return true;
                                }
                            }

                            break;
                        }
                }

                identifier = null;
                noTracking = false;

                return false;
            }
        }
    }
}

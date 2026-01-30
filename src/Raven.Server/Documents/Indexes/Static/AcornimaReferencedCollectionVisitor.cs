using System;
using System.Collections.Generic;
using Acornima.Ast;
using Raven.Server.Documents.AI.Embeddings;
using Sparrow;

namespace Raven.Server.Documents.Indexes.Static
{
    public sealed class AcornimaReferencedCollectionVisitor : AcornimaVisitor
    {
        public readonly HashSet<CollectionName> ReferencedCollection = new HashSet<CollectionName>();
        public bool HasLoadVector { get; private set; }
        
        public bool LoadVectorIsUsingDefaultCollection { get; private set; }
        
        public bool HasCreateVector { get; private set; }
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
                    case JavaScriptIndex.LoadVectorMethodName:
                        PortableExceptions.ThrowIf<ArgumentException>(callExpression.Arguments.Count is not (2 or 4), $"loadVector method is expecting two or four arguments, the path to the vector in Embeddings Generation tasks. e.g. loadVector('embeddingsGenerationTaskIdentifier', 'path') or loadVector('embeddingsGenerationTaskIdentifier', 'path', doc.SourceDocumentId, 'SourceDocumentCollectionName') but was invoked with {callExpression.Arguments.Count} arguments.");

                        if (callExpression.Arguments.Count == 4)
                        {
                            var collection = callExpression.Arguments[3];
                            if (collection is Literal { Value: string s })
                            {
                                ReferencedCollection.Add(new CollectionName(EmbeddingsHelper.GetEmbeddingDocumentCollectionName(s)));
                            }
                        }
                        else
                        {
                            LoadVectorIsUsingDefaultCollection = true;
                        }
                        
                        HasLoadVector = true;
                        break;
                        
                    case JavaScriptIndex.CreateVectorMethodName:
                        HasCreateVector = true;
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

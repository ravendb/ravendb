using System;
using Lucene.Net.Documents;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public sealed class MethodDetectorRewriter : CSharpSyntaxRewriter
    {
        public readonly IndexCompiler.IndexMethods Methods = new IndexCompiler.IndexMethods();

        private long IndexVersion { get; set; }
        
        public MethodDetectorRewriter(long indexVersion)
        {
            IndexVersion = indexVersion;
        }

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var expression = node.Expression.ToString();
            if (NoTrackingRewriter.TryRemoveNoTracking(expression, out var newExpression))
                expression = newExpression;

            switch (expression)
            {
                case "this.LoadDocument":
                case nameof(AbstractCommonApiForIndexes.LoadDocument):
                    AssertLoadDocumentHasConstantCollectionName(node);
                    Methods.HasLoadDocument = true;
                    break;
                case "this.TransformWith":
                case "TransformWith":
                    Methods.HasTransformWith = true;
                    break;
                case "this.Include":
                case "Include":
                    Methods.HasInclude = true;
                    break;
                case "results.GroupBy":
                    Methods.HasGroupBy = true;
                    break;
                case "this.CreateField":
                case "CreateField":
                    Methods.HasCreateField = true;
                    break;
            }

            var memberAccessExpression = node.Expression as MemberAccessExpressionSyntax;

            if (memberAccessExpression != null)
            {
                switch (memberAccessExpression.Name.Identifier.Text)
                {
                    case "Boost":
                        Methods.HasBoost = true;
                        break;
                }
            }

            return base.VisitInvocationExpression(node);
        }

        public override SyntaxNode VisitObjectCreationExpression(ObjectCreationExpressionSyntax node)
        {
            var type = node.Type.ToString();
            if (type == nameof(Field) || type == nameof(NumericField) || type == typeof(Field).FullName || type == typeof(NumericField).FullName)
                Methods.HasCreateField = true;

            return base.VisitObjectCreationExpression(node);
        }

        public override SyntaxNode VisitGroupClause(GroupClauseSyntax node)
        {
            Methods.HasGroupBy = true;
            return base.VisitGroupClause(node);
        }

        private void AssertLoadDocumentHasConstantCollectionName(InvocationExpressionSyntax node)
        {
            var collectionExpression = node.ArgumentList.Arguments[1].Expression;
                
            if (collectionExpression is not LiteralExpressionSyntax && IndexVersion >= IndexDefinitionBaseServerSide.IndexVersion.LoadDocumentWithDynamicCollectionNameShouldThrow)
                throw new ArgumentException("LoadDocument method has to be called with constant value as collection name");
        }
    }
}

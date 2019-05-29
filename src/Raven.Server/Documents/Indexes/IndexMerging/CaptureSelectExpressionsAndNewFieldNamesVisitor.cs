using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.IndexMerging
{
    internal class CaptureSelectExpressionsAndNewFieldNamesVisitor : CaptureSelectNewFieldNamesVisitor
    {
        private readonly bool _outerMostRequired;
        private readonly HashSet<string> _fieldNames;
        private readonly Dictionary<string, ExpressionSyntax> _selectExpressions;
        private bool _queryProcessed;

        public CaptureSelectExpressionsAndNewFieldNamesVisitor(bool outerMostRequired, HashSet<string> fieldNames, Dictionary<string, ExpressionSyntax> selectExpressions)
        {
            _outerMostRequired = outerMostRequired;
            _fieldNames = fieldNames;
            _selectExpressions = selectExpressions;
        }

        public override SyntaxNode VisitAnonymousObjectCreationExpression(AnonymousObjectCreationExpressionSyntax node)
        {
            // we only want the outer most value
            if (_queryProcessed && _outerMostRequired)
                return node;

            _fieldNames.Clear();
            _selectExpressions.Clear();

            _queryProcessed = true;

            foreach (var initializer in node.Initializers)
            {
                CollectFieldNamesAndSelectsFromMemberDeclarator(initializer);
            }

            return node;
        }

        private void CollectFieldNamesAndSelectsFromMemberDeclarator(AnonymousObjectMemberDeclaratorSyntax expression)
        {
            string name;

            if (expression.NameEquals != null)
            {
                name = expression.NameEquals.Name.Identifier.ValueText;
            }

            else if (expression.Expression is MemberAccessExpressionSyntax memberAccess)
            { 
                name = memberAccess.Name.Identifier.ValueText;
            }

            else return;

            _fieldNames.Add(name);
            _selectExpressions[name] = expression.Expression;
        }
    }
}

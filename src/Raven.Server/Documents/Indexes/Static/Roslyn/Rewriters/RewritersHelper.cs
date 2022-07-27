using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public static class RewritersHelper
    {
        public static HashSet<CompiledIndexField> ExtractFields(AnonymousObjectCreationExpressionSyntax anonymousObjectCreationExpressionSyntax, bool retrieveOriginal = false, bool nestFields = false)
        {
            var fields = new HashSet<CompiledIndexField>();
            for (var i = 0; i < anonymousObjectCreationExpressionSyntax.Initializers.Count; i++)
            {
                var initializer = anonymousObjectCreationExpressionSyntax.Initializers[i];
                string name;
                if (initializer.NameEquals != null && retrieveOriginal == false)
                {
                    name = initializer.NameEquals.Name.Identifier.Text;
                }
                else
                {
                    if (initializer.Expression is MemberAccessExpressionSyntax memberAccessExpressionSyntax)
                    {
                        fields.Add(ExtractField(memberAccessExpressionSyntax, nestFields));
                        continue;
                    }

                    var identifierNameSyntax = initializer.Expression as IdentifierNameSyntax;

                    if (identifierNameSyntax == null)
                        throw new NotSupportedException($"Cannot extract field name from: {initializer}");

                    name = identifierNameSyntax.Identifier.Text;
                }

                fields.Add(new SimpleField(name));
            }

            return fields;
        }

        public static CompiledIndexField ExtractField(MemberAccessExpressionSyntax expression, bool nestFields = true)
        {
            var name = expression.Name.Identifier.Text;

            string[] path = null;
            if (nestFields)
                path = ExtractPath(expression);

            if (path == null || path.Length <= 1)
                return new SimpleField(name);

            return new NestedField(path[0], path.Skip(1).ToArray());
        }

        private static string[] ExtractPath(MemberAccessExpressionSyntax expression)
        {
            var path = expression.ToString().Split(".");

            return path
                .Skip(1)                // skipping variable name e.g. 'result'
                .ToArray();
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    internal class FieldNamesValidator
    {
        private readonly CaptureSelectNewFieldNamesVisitor _visitor = new CaptureSelectNewFieldNamesVisitor();

        private HashSet<Field> _baseFields;

        private string _baseFunction;

        public bool Validate(string indexingFunction, ExpressionSyntax expression, bool throwOnError = true)
        {
            _visitor.Fields = null;
            _visitor.Visit(expression);

            if (_visitor.Fields == null || _visitor.Fields.Count == 0)
                throw new InvalidOperationException($"Could not extract any fields from '{indexingFunction}'.");

            if (_baseFields == null)
            {
                _baseFunction = indexingFunction;
                _baseFields = _visitor.Fields;
                return true;
            }

            if (_baseFields.SetEquals(_visitor.Fields))
                return true;
            
            if (throwOnError)
            {
                var message = $@"Map and Reduce functions of a index must return identical types.
Baseline function		: {_baseFunction}
Non matching function	: {indexingFunction}

Common fields			: {string.Join(", ", _baseFields.Intersect(_visitor.Fields))}
Missing fields			: {string.Join(", ", _baseFields.Except(_visitor.Fields))}
Additional fields		: {string.Join(", ", _visitor.Fields.Except(_baseFields))}";

                throw new InvalidOperationException(message);
            }

            return false;
        }

        public Field[] Fields => _baseFields.ToArray();

        public Field[] ExtractedFields => _visitor.Fields.ToArray();
    }
}

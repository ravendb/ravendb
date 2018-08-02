using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class PatternMatchElementExpression : PatternMatchExpression
    {
        public string FromAlias;

        public string EdgeAlias;

        public string ToAlias;

        public override string ToString() => GetText();

        public override string GetText(IndexQueryServerSide parent) => GetText();

        public string GetText() => $"{FromAlias}-{EdgeAlias}->{ToAlias}";
    }
}

using System;
using System.Text;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class PatternMatchElementExpression : QueryExpression
    {
        public MatchPath[] Path;

        public override string ToString() => GetText();

        public override string GetText(IndexQueryServerSide parent) => GetText();

        public string GetText()
        {
            var sb = new StringBuilder();

            for (int i = 0; i < Path.Length; i++)
            {
                sb.Append(Path[i].IsEdge ? "[" : "(");
                sb.Append(Path[i].Alias);
                sb.Append(Path[i].IsEdge ? "]" : ")");

                if (i + 1 < Path.Length)
                {
                    switch (Path[i + 1].EdgeType)
                    {
                        case EdgeType.Outgoing:
                            sb.Append(Path[i+1].IsEdge ? "-" : "->");
                            break;
                        case EdgeType.Incoming:
                            sb.Append(Path[i].IsEdge ? "-" : "<-");
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(Path[i + 1].EdgeType + " is not known");
                    }
                }
            }
            return sb.ToString();
        }

        public override bool Equals(QueryExpression other)
        {
            if (!(other is PatternMatchElementExpression pe))
                return false;

            if (Path.Length != pe.Path.Length)
                return false;

            for (int i = 0; i < Path.Length; i++)
            {
                if (Path[i].EdgeType != pe.Path[i].EdgeType ||
                    Path[i].Alias != pe.Path[i].Alias)
                    return false;
            }
            return true;
        }
    }

    public enum EdgeType
    {
        Outgoing,
        Incoming
    }

    public struct MatchPath
    {
        public StringSegment Alias;
        public EdgeType EdgeType;
        public bool IsEdge;
    }
}

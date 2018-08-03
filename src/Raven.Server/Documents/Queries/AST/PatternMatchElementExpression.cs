using System;
using System.Text;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class PatternMatchElementExpression : QueryExpression
    {
        public (StringSegment Alias, EdgeType EdgeType)[] Path;

        public override string ToString() => GetText();

        public override string GetText(IndexQueryServerSide parent) => GetText();

        public string GetText()
        {
            var sb = new StringBuilder();

            for (int i = 0; i < Path.Length; i++)
            {
                sb.Append("{");
                sb.Append(Path[i].Alias);
                sb.Append("}");

                if (i + 1 < Path.Length)
                {
                    switch (Path[i + 1].EdgeType)
                    {
                        case EdgeType.Outgoing:
                            sb.Append("->");
                            break;
                        case EdgeType.Incoming:
                            sb.Append("<-");
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(Path[i + 1].EdgeType + " is not known");
                    }
                }
            }
            return sb.ToString();
        }
    }

    public enum EdgeType
    {
        Outgoing,
        Incoming
    }
}

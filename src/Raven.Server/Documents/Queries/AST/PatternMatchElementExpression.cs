using System;
using System.Collections.Generic;
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
                    if(Path[i+1].Recursive != null)
                    {
                        sb.Append(Path[i + 1].Recursive);
                        continue;
                    }

                    switch (Path[i + 1].EdgeType)
                    {
                        case EdgeType.Right:
                            sb.Append(Path[i+1].IsEdge ? "-" : "->");
                            break;
                        case EdgeType.Left:
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
        Right,
        Left
    }

    public struct RecursiveMatch
    {
        public StringSegment Alias;
        public List<MatchPath> Pattern;
        public HashSet<StringSegment> Aliases;
        public int? Min;
        public int? Max;

        public override string ToString()
        {
            var sp = new StringBuilder(" recursive ");
            if (Alias.Length != 0)
                sp.Append(" as ").Append(Alias).Append(" ");

            sp.Append("{ ");

            foreach (var item in Pattern)
            {
                sp.Append(item);
                sp.Append(" ");
            }

            sp.Append("} ");

            return sp.ToString();
        }
    }

    public struct MatchPath
    {
        public StringSegment Alias;
        public EdgeType EdgeType;
        public bool IsEdge;
        public RecursiveMatch? Recursive;

        public override string ToString()
        {
            if(Recursive != null)
            {
                return Recursive.Value.ToString();
            }

            return (IsEdge ? "[" : "(") +  Alias + (IsEdge ? "]" : ")") + (EdgeType == EdgeType.Left ? "<-" : "->"); 
        }
    }
}

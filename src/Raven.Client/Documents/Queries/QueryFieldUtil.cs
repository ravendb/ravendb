using System.Text;

namespace Raven.Client.Documents.Queries
{
    public static class QueryFieldUtil
    {
        public static string EscapeIfNecessary(string name)
        {
            return EscapeIfNecessary(name, isPath: false);
        }

        public static string EscapeIfNecessary(string name, bool isPath)
        {
            if (string.IsNullOrEmpty(name) ||
                name == Constants.Documents.Indexing.Fields.DocumentIdFieldName ||
                name == Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName ||
                name == Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName ||
                name == Constants.Documents.Indexing.Fields.ValueFieldName ||
                name == Constants.Documents.Indexing.Fields.SpatialShapeFieldName)
                return name;

            if (ShouldEscape(name) == false)
                return name;
            
            var sb = new StringBuilder(name);
            var needEndQuote = false;
            var lastTermStart = 0;

            for (int i = 0; i < sb.Length; i++)
            {
                var c = sb[i];
                if ( i == 0 && char.IsLetter(c) == false && c != '_' && c != '@')
                {
                    sb.Insert(lastTermStart, '"');
                    needEndQuote = true;
                    continue;
                }

                if (isPath && c == '.')
                {
                    if (needEndQuote)
                    {
                        needEndQuote = false;
                        sb.Insert(i, '\'');
                        i++;
                    }

                    lastTermStart = i+1;
                    continue;
                }
                
                if (char.IsLetterOrDigit(c) == false && c != '_' && c != '-' && c != '@' && c != '.' && c != '[' && c != ']' && needEndQuote == false)
                {
                    sb.Insert(lastTermStart, '\'');
                    needEndQuote = true;
                    continue;
                }
            }

            if (needEndQuote)
            {
                sb.Append('\'');
            }

            return sb.ToString();

                bool ShouldEscape(string s)
            {
                var escape = false;

                bool insideEscaped = false;

                for (var i = 0; i < s.Length; i++)
                {
                    var c = s[i];

                    if (c == '\'' || c == '"')
                    {
                        insideEscaped = !insideEscaped;
                        continue;
                    }

                    if (i == 0)
                    {
                        if (char.IsLetter(c) == false && c != '_' && c != '@' && insideEscaped == false)
                        {
                            escape = true;
                            break;
                        }
                    }
                    else
                    {
                        if (char.IsLetterOrDigit(c) == false && c != '_' && c != '-' && c != '@' && c != '.' && c != '[' && c != ']' && insideEscaped == false)
                        {
                            escape = true;
                            break;
                        }

                        if (isPath && c == '.' && insideEscaped == false)
                        {
                            escape = true;
                            break;
                        }
                    }
                }

                escape |= insideEscaped;
                return escape;
            }
        }
    }
}

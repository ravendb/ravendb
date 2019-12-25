namespace Raven.Client.Documents.Queries
{
    public static class QueryFieldUtil
    {
        public static string EscapeIfNecessary(string name, bool path = false)
        {
            if (string.IsNullOrEmpty(name) || 
                name == Constants.Documents.Indexing.Fields.DocumentIdFieldName || 
                name == Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName || 
                name == Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName || 
                name == Constants.Documents.Indexing.Fields.SpatialShapeFieldName )
                return name;

            var escape = false;

            bool insideEscaped = false;

            for (var i = 0; i < name.Length; i++)
            {
                var c = name[i];

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

                    if (path && c == '.' && insideEscaped == false)
                    {
                        escape = true;
                        break;
                    }
                }
            }

            if (escape || insideEscaped)
                return $"'{name}'";

            return name;
        }
    }
}

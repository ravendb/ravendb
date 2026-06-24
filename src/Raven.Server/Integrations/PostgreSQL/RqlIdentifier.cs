namespace Raven.Server.Integrations.PostgreSQL
{
    internal static class RqlIdentifier
    {
        // Guards names spliced where RQL/JS quoting isn't applied: verbatim grouped/aggregate RQL that is also
        // matched case-insensitively, and JavaScript projection members/values (`select { a: a }`, `alias.field`).
        // Must be identifier-shaped - letter/underscore then letters/digits/underscores (Unicode letters allowed).
        // Other RQL positions quote untrusted names via QueryFieldUtil.EscapeIfNecessary.
        public static bool IsSafe(string s)
        {
            if (string.IsNullOrWhiteSpace(s))
                return false;
            if (char.IsLetter(s[0]) == false && s[0] != '_')
                return false;
            for (int i = 1; i < s.Length; i++)
            {
                var c = s[i];
                if (char.IsLetterOrDigit(c) == false && c != '_')
                    return false;
            }

            return true;
        }
    }
}

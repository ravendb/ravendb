using System.IO;
using System.Text;
using Sparrow;

namespace Raven.Server.Extensions
{
    public static class StringExtensions
    {
        public static bool IsIdentifier(this StringSegment token)
        {
            return Raven.Client.Extensions.StringExtensions.IsIdentifier(token.Buffer, token.Offset, token.Length);
        }

        public static string NormalizeLineEnding(this string script)
        {
            var sb = new StringBuilder();
            using (var reader = new StringReader(script))
            {
                while (true)
                {
                    var line = reader.ReadLine();
                    if (line == null)
                        return sb.ToString();
                    sb.AppendLine(line);
                }
            }
        }
        
        public static string ToUpperFirstLetter(this string self)
        {
            if (string.IsNullOrEmpty(self))
                return string.Empty;

            var chars = self.ToCharArray();
            chars[0] = char.ToUpper(chars[0]);

            return new string(chars);
        }

        public static bool IsUpperLettersOnly(this string nodeTag)
        {
            foreach (var c in nodeTag)
            {
                if (char.IsUpper(c) == false)
                    return false;
            }
            return true;
        }
    }
}

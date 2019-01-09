using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Extensions.Primitives;

namespace Raven.Server.Extensions
{
    public static class StringExtensions
    {
        public static List<string> GetSemicolonSeparatedValues(this string self)
        {
            return self.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(x => x.Trim())
                .ToList();
        }

        public static bool IsIdentifier(this StringSegment token)
        {
            if (token.Length == 0)
                return false;

            if (token.Length == 1)
                return char.IsLetter(token[0]) || token[0] == '_';
            
            if (!char.IsLetter(token[0]) && token[0] != '_')
                return false;

            for (int i = 1; i < token.Length; i++)
            {
                if (!char.IsLetterOrDigit(token[i]) && token[i] != '_')
                    return false;
            }

            return true;
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

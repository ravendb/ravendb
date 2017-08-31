using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

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
    }
}

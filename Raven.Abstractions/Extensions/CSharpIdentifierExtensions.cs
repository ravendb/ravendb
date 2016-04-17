using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Extensions
{
    public static class CSharpIdentifierExtensions
    {
        public static bool IsValidIdentifier(this string identifier)
        {
            if (string.IsNullOrEmpty(identifier))
                return false;

            if (keywordsTable == null)
                FillKeywordTable();

            if (keywordsTable.Contains(identifier))
                return false;

            if (!is_identifier_start_character(identifier[0]))
                return false;

            for (int i = 1; i < identifier.Length; i++)
                if (!is_identifier_part_character(identifier[i]))
                    return false;

            return true;
        }


        internal static bool is_identifier_start_character(char c)
        {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '@' || char.IsLetter(c);
        }

        internal static bool is_identifier_part_character(char c)
        {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || (c >= '0' && c <= '9') || char.IsLetter(c);
        }


        private static System.Collections.Hashtable keywordsTable;
        private static string[] keywords = new string[] {
    "abstract","event","new","struct","as","explicit","null","switch","base","extern",
    "this","false","operator","throw","break","finally","out","true",
    "fixed","override","try","case","params","typeof","catch","for",
    "private","foreach","protected","checked","goto","public",
    "unchecked","class","if","readonly","unsafe","const","implicit","ref",
    "continue","in","return","using","virtual","default",
    "interface","sealed","volatile","delegate","internal","do","is",
    "sizeof","while","lock","stackalloc","else","static","enum",
    "namespace",
    "object","bool","byte","float","uint","char","ulong","ushort",
    "decimal","int","sbyte","short","double","long","string","void",
    "partial", "yield", "where"
};

        internal static void FillKeywordTable()
        {
            lock (keywords)
            {
                if (keywordsTable == null)
                {
                    keywordsTable = new System.Collections.Hashtable();
                    foreach (string keyword in keywords)
                    {
                        keywordsTable.Add(keyword, keyword);
                    }
                }
            }
        }
    }
}

//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;

namespace Raven.NormalizeCrLF
{
    internal class Program
    {
        private static void Main()
        {
            var files = Directory.EnumerateFiles(".", "*.cs", SearchOption.AllDirectories)
                .Concat(Directory.EnumerateFiles(".", "*.js", SearchOption.AllDirectories))
                .Concat(Directory.EnumerateFiles(".", "*.html", SearchOption.AllDirectories))
                .Concat(Directory.EnumerateFiles(".", "*.csproject", SearchOption.AllDirectories))
                .Concat(Directory.EnumerateFiles(".", "*.sln", SearchOption.AllDirectories));
            foreach (var file in files)
            {
                var content = File.ReadAllText(file);
                var lines = content.Split(new[] {"\r\n", "\r", "\n"}, StringSplitOptions.None);
                var updatedContent = string.Join("\r\n", lines);
                if (string.Equals(content, updatedContent))
                    continue;
                Console.WriteLine("Updating {0}", file);
                File.WriteAllText(file, updatedContent);
            }
        }
    }
}
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace Raven.Samples.PrepareForRelease
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var slnPath = args[0];
                var libPath = args[1];
                Environment.CurrentDirectory = Path.GetDirectoryName(slnPath);

                RemoveProjectReferencesNotInSameDirectory(slnPath);

                var ns = XNamespace.Get("http://schemas.microsoft.com/developer/msbuild/2003");
                foreach (var projectFile in Directory.GetFiles(Path.GetDirectoryName(slnPath), "*.csproj", SearchOption.AllDirectories))
                {
                    var prj = XDocument.Load(projectFile);

                    foreach (var reference in prj.Descendants(ns + "Reference").ToArray())
                    {
                        var hintPath = reference.Element(ns + "HintPath");
                        if (hintPath == null)
                            continue;
                        var guessFileName = GuessFileName(Path.GetFileName(hintPath.Value), libPath);
                        if(guessFileName == null)
                            continue;
                        hintPath.Value = Path.Combine(@"..\..\EmbeddedClient", Path.GetFileName(hintPath.Value));
                    }

                    foreach (var prjRef in prj.Descendants(ns + "ProjectReference").ToArray())
                    {
                        if (prjRef.Attribute("Include").Value.StartsWith(@"..\..\") == false)
                            continue;
                        var refName = prjRef.Element(ns + "Name").Value;
                        prjRef.Parent.Add(
                            new XElement(ns + "Reference",
                                new XAttribute("Include", refName),
                                new XElement(ns+"HintPath", GuessFileName(refName, libPath))
                                )
                            );

                        prjRef.Remove();
                    }
                   
                    prj.Save(projectFile);
                }
            }
            catch(Exception e)
            {
                Console.Error.WriteLine(e);
                Environment.Exit(-1);
            }


        }

        private static string GuessFileName(string refName, string libPath)
        {
            var searchPattern = Path.GetExtension(refName) == ".dll" ? refName : refName + "*.*";
            var firstOrDefault = Directory.GetFiles(libPath, searchPattern, SearchOption.AllDirectories).FirstOrDefault();
            if (firstOrDefault == null)
                return null;
            return firstOrDefault;
        }

        private static void RemoveProjectReferencesNotInSameDirectory(string path)
        {
            var lastLineHadReferenceToParentDirectory = false;
            var slnLines = File.ReadAllLines(path)
                .Where(line =>
                {
                    if (lastLineHadReferenceToParentDirectory)
                    {
                        lastLineHadReferenceToParentDirectory = false;
                        return false;
                    }
                    return (lastLineHadReferenceToParentDirectory = line.Contains("..")) == false;
                });

            File.WriteAllLines(path, slnLines);
        }
    }
}

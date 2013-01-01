//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
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
					Console.WriteLine("Preparing project file: " +projectFile);
					var prj = XDocument.Load(projectFile);

					foreach (var reference in prj.Descendants(ns + "Reference").ToArray())
					{
						var hintPath = reference.Element(ns + "HintPath");
						if (hintPath == null)
							continue;
						var guessFileName = GuessFileName(Path.GetFileName(hintPath.Value), libPath, true);
						if(guessFileName == null)
							continue;
						hintPath.Value = Path.Combine(@"..\..", guessFileName);
					}

					foreach (var prjRef in prj.Descendants(ns + "ProjectReference").ToArray())
					{
						var includeAttrib = prjRef.Attribute("Include");
						if (includeAttrib == null)
							continue;
						if (includeAttrib.Value.StartsWith(@"..\..\") == false)
							continue;
						var prjRefName = prjRef.Element(ns + "Name");
						if(prjRefName == null)
							continue;
						var refName = prjRefName.Value;
						var parent = prjRef.Parent;
						if(parent == null)
							continue;
						parent.Add(
							new XElement(ns + "Reference",
								new XAttribute("Include", refName),
								new XElement(ns+"HintPath", Path.Combine(@"..\..", GuessFileName(refName, libPath, false)))
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

		private static string GuessFileName(string refName, string libPath, bool allowMissingFiles)
		{
			var fullPath = Path.GetFullPath(libPath);
			var searchPattern = Path.GetExtension(refName) == ".dll" ? refName : refName + ".*";
			var filePath = Directory.GetFiles(fullPath, searchPattern, SearchOption.AllDirectories)
				.Where(x=>x.ToUpperInvariant().Contains("SAMPLES") == false)
				.FirstOrDefault();
			if (filePath == null)
			{
				if (allowMissingFiles)
					return null;
				throw new InvalidOperationException("Could not file a file matching '" + searchPattern + "' in: " +
					libPath);
			}
			filePath = Path.GetFullPath(filePath);
			return filePath.Substring(fullPath.Length + 1);
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

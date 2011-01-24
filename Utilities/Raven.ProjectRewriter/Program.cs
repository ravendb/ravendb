//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using System.Reflection;
using System.Xml;
using System.Xml.Linq;

namespace Raven.ProjectRewriter
{
    class Program
    {
        static void Main(string[] args)
        {
            var xmlns = XNamespace.Get("http://schemas.microsoft.com/developer/msbuild/2003");
            if (args.Length == 1 && args[0] == "commercial")
                MarkDatabaseProjectAsCommercial(xmlns);
            Generate35AbstractionsProject(xmlns);
            Generate35ClientProject(xmlns);
        }

        private static void Generate35AbstractionsProject(XNamespace xmlns)
        {
            var database = XDocument.Load(@"Raven.Abstractions\Raven.Abstractions.csproj");
            foreach (var element in database.Root.Descendants(xmlns + "DefineConstants").ToArray())
            {
                if (element.Value.EndsWith(";") == false)
                    element.Value += ";";
                element.Value += "NET_3_5";
            }
            foreach (var element in database.Root.Descendants(xmlns + "Reference").ToArray())
            {
                if (element.Attribute("Include").Value == "Microsoft.CSharp")
                    element.Remove();
                if (element.Attribute("Include").Value == "System.ComponentModel.Composition")
                    element.Remove();
                if (element.Attribute("Include").Value == "Newtonsoft.Json")
                {
                    element.Attribute("Include").Value = "Newtonsoft.Json.Net35";
                    element.Element(xmlns+"HintPath").Value = @"..\SharedLibs\Newtonsoft.Json.Net35.dll";
                }
            }
            foreach (var element in database.Root.Descendants(xmlns + "DocumentationFile").ToArray())
            {
                element.Value = element.Value.Replace(".XML", "-3.5.XML");
            }
            foreach (var element in database.Root.Descendants(xmlns + "TargetFrameworkVersion"))
            {
                element.Value = "v3.5";
            }
            foreach (var element in database.Root.Descendants(xmlns + "AssemblyName"))
            {
                element.Value += "-3.5";
            }
            using (var xmlWriter = XmlWriter.Create(@"Raven.Abstractions\Raven.Abstractions.g.3.5.csproj",
                                                    new XmlWriterSettings
                                                    {
                                                        Indent = true
                                                    }))
            {
                database.WriteTo(xmlWriter);
                xmlWriter.Flush();
            }
        }

        private static void Generate35ClientProject(XNamespace xmlns)
        {
            var database = XDocument.Load(@"Raven.Client.Lightweight\Raven.Client.Lightweight.csproj");
            foreach (var element in database.Root.Descendants(xmlns + "DefineConstants").ToArray())
            {
                if (element.Value.EndsWith(";") == false)
                    element.Value += ";";
                element.Value += "NET_3_5";
            }
            foreach (var element in database.Root.Descendants(xmlns + "ProjectReference").ToArray())
            {
                if (element.Element(xmlns + "Name").Value != "Raven.Abstractions")
                    continue;
                element.Attribute("Include").Value = element.Attribute("Include").Value.Replace(".csproj", ".g.3.5.csproj");
                element.Element(xmlns + "Name").Value += "-3.5";
            }

            foreach (var element in database.Root.Descendants(xmlns + "Reference").ToArray())
            {
                if (element.Attribute("Include").Value == "Microsoft.CSharp")
                    element.Remove();
                if (element.Attribute("Include").Value == "System.ComponentModel.Composition")
                    element.Remove();
                if (element.Attribute("Include").Value == "Newtonsoft.Json")
                {
                    element.Attribute("Include").Value = "Newtonsoft.Json.Net35";
                    element.Element(xmlns + "HintPath").Value = @"..\SharedLibs\Newtonsoft.Json.Net35.dll";
                }
            }

            foreach (var element in database.Root.Descendants(xmlns + "DocumentationFile").ToArray())
            {
                element.Value = element.Value.Replace(".XML", "-3.5.XML");
            }
            foreach (var element in database.Root.Descendants(xmlns + "TargetFrameworkVersion"))
            {
                element.Value = "v3.5";
            }
            foreach (var element in database.Root.Descendants(xmlns + "AssemblyName"))
            {
                element.Value += "-3.5";
            }
            using (var xmlWriter = XmlWriter.Create(@"Raven.Client.Lightweight\Raven.Client.Lightweight.g.3.5.csproj",
                                                    new XmlWriterSettings
                                                    {
                                                        Indent = true
                                                    }))
            {
                database.WriteTo(xmlWriter);
                xmlWriter.Flush();
            }
        }

        private static void MarkDatabaseProjectAsCommercial(XNamespace xmlns)
        {
            var database = XDocument.Load(@"Raven.Database\Raven.Database.csproj");
            foreach (var element in database.Root.Descendants(xmlns + "DefineConstants").ToArray())
            {
                if (element.Value.EndsWith(";") == false)
                    element.Value += ";";
                element.Value += "COMMERCIAL";
            }
            using (var xmlWriter = XmlWriter.Create(@"Raven.Database\Raven.Database.g.csproj",
                                                    new XmlWriterSettings
                                                    {
                                                        Indent = true
                                                    }))
            {
                database.WriteTo(xmlWriter);
                xmlWriter.Flush();
            }
        }
    }
}

//-----------------------------------------------------------------------
// <copyright file="AbstractIndexCreationTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Indexes
{
    public class AdditionalAssembly : IDynamicJson
    {
        public string AssemblyName { get; private set; }

        public string AssemblyPath { get; private set; }

        public string PackageName { get; private set; }

        public string PackageVersion { get; private set; }

        public string PackageSourceUrl { get; private set; }

        public HashSet<string> Usings { get; private set; }

        private AdditionalAssembly()
        {
        }

        public override bool Equals(object obj)
        {
            return obj is AdditionalAssembly assembly &&
                   AssemblyName == assembly.AssemblyName &&
                   AssemblyPath == assembly.AssemblyPath &&
                   PackageName == assembly.PackageName &&
                   PackageVersion == assembly.PackageVersion &&
                   PackageSourceUrl == assembly.PackageSourceUrl &&
                   EqualityComparer<HashSet<string>>.Default.Equals(Usings, assembly.Usings);
        }

        public override int GetHashCode()
        {
            int hashCode = -585367404;
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(AssemblyName);
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(AssemblyPath);
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(PackageName);
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(PackageVersion);
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(PackageSourceUrl);
            hashCode = hashCode * -1521134295 + EqualityComparer<HashSet<string>>.Default.GetHashCode(Usings);
            return hashCode;
        }

        public override string ToString()
        {
            return $"Additional Assembly. {nameof(AssemblyName)}: {AssemblyName}, {nameof(AssemblyPath)}: {AssemblyPath}, {nameof(PackageName)}: {PackageName}, {nameof(PackageVersion)}: {PackageVersion}, {nameof(PackageSourceUrl)}: {PackageSourceUrl}";
        }

        internal AdditionalAssembly Clone()
        {
            return new AdditionalAssembly
            {
                AssemblyName = AssemblyName,
                AssemblyPath = AssemblyPath,
                PackageName = PackageName,
                PackageVersion = PackageVersion,
                PackageSourceUrl = PackageSourceUrl,
                Usings = Usings != null ? new HashSet<string>(Usings) : null
            };
        }

        public static AdditionalAssembly OnlyUsings(HashSet<string> usings)
        {
            if (usings == null || usings.Count == 0)
                throw new ArgumentNullException(nameof(usings));

            return new AdditionalAssembly
            {
                Usings = usings
            };
        }

        public static AdditionalAssembly FromRuntime(string assemblyName, HashSet<string> usings = null)
        {
            if (string.IsNullOrWhiteSpace(assemblyName))
                throw new ArgumentException($"'{nameof(assemblyName)}' cannot be null or whitespace", nameof(assemblyName));

            return new AdditionalAssembly
            {
                AssemblyName = assemblyName,
                Usings = usings
            };
        }

        public static AdditionalAssembly FromPath(string assemblyPath, HashSet<string> usings = null)
        {
            if (string.IsNullOrWhiteSpace(assemblyPath))
                throw new ArgumentException($"'{nameof(assemblyPath)}' cannot be null or whitespace", nameof(assemblyPath));

            return new AdditionalAssembly
            {
                AssemblyPath = assemblyPath,
                Usings = usings
            };
        }

        public static AdditionalAssembly FromNuGet(string packageName, string packageVersion, string packageSourceUrl = null, HashSet<string> usings = null)
        {
            if (string.IsNullOrWhiteSpace(packageName))
                throw new ArgumentException($"'{nameof(packageName)}' cannot be null or whitespace", nameof(packageName));

            if (string.IsNullOrWhiteSpace(packageVersion))
                throw new ArgumentException($"'{nameof(packageVersion)}' cannot be null or whitespace", nameof(packageVersion));

            return new AdditionalAssembly
            {
                PackageName = packageName,
                PackageVersion = packageVersion,
                PackageSourceUrl = packageSourceUrl,
                Usings = usings
            };
        }

        public virtual DynamicJsonValue ToJson()
        {
            var djv = new DynamicJsonValue();
            if (AssemblyName != null)
                djv[nameof(AssemblyName)] = AssemblyName;

            if (AssemblyPath != null)
                djv[nameof(AssemblyPath)] = AssemblyPath;

            if (PackageName != null)
                djv[nameof(PackageName)] = PackageName;

            if (PackageVersion != null)
                djv[nameof(PackageVersion)] = PackageVersion;

            if (PackageSourceUrl != null)
                djv[nameof(PackageSourceUrl)] = PackageSourceUrl;

            if (Usings != null && Usings.Count > 0)
                djv[nameof(Usings)] = new DynamicJsonArray(Usings);

            return djv;
        }

        internal void WriteTo(AbstractBlittableJsonTextWriter writer)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(AssemblyName));
            writer.WriteString(AssemblyName);
            writer.WriteComma();

            writer.WritePropertyName(nameof(AssemblyPath));
            writer.WriteString(AssemblyPath);
            writer.WriteComma();

            writer.WritePropertyName(nameof(PackageName));
            writer.WriteString(PackageName);
            writer.WriteComma();

            writer.WritePropertyName(nameof(PackageVersion));
            writer.WriteString(PackageVersion);
            writer.WriteComma();

            writer.WritePropertyName(nameof(PackageSourceUrl));
            writer.WriteString(PackageSourceUrl);
            writer.WriteComma();

            writer.WriteArray(nameof(Usings), Usings);

            writer.WriteEndObject();
        }
    }
}

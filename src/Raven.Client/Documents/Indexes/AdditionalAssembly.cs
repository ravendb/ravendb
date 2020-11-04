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

        protected bool Equals(AdditionalAssembly other)
        {
            return string.Equals(AssemblyName, other.AssemblyName, StringComparison.OrdinalIgnoreCase)
                   && string.Equals(AssemblyPath, other.AssemblyPath, StringComparison.OrdinalIgnoreCase)
                   && string.Equals(PackageName, other.PackageName, StringComparison.OrdinalIgnoreCase)
                   && string.Equals(PackageVersion, other.PackageVersion, StringComparison.OrdinalIgnoreCase)
                   && string.Equals(PackageSourceUrl, other.PackageSourceUrl, StringComparison.OrdinalIgnoreCase)
                   && (Usings != null && other.Usings != null && Usings.SetEquals(other.Usings));
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((AdditionalAssembly)obj);
        }

        public override int GetHashCode()
        {
            var hashCode = new HashCode();
            hashCode.Add(AssemblyName, StringComparer.OrdinalIgnoreCase);
            hashCode.Add(AssemblyPath, StringComparer.OrdinalIgnoreCase);
            hashCode.Add(PackageName, StringComparer.OrdinalIgnoreCase);
            hashCode.Add(PackageVersion, StringComparer.OrdinalIgnoreCase);
            hashCode.Add(PackageSourceUrl, StringComparer.OrdinalIgnoreCase);

            if (Usings != null)
            {
                foreach (var @using in Usings)
                    hashCode.Add(@using);
            }
            else
            {
                hashCode.Add(Usings);
            }

            return hashCode.ToHashCode();
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

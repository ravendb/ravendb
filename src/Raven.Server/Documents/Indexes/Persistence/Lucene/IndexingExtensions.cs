//-----------------------------------------------------------------------
// <copyright file="IndexingExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using System.Reflection;
using System.Runtime.Loader;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Raven.Server.Documents.Indexes.Analysis;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public static class IndexingExtensions
    {
        private static readonly Assembly LuceneAssembly = typeof(StandardAnalyzer).Assembly;

        private static readonly Type[] ConstructorParameterTypes = { typeof(global::Lucene.Net.Util.Version) };

        private static readonly object[] ConstructorParameterValues = { global::Lucene.Net.Util.Version.LUCENE_30 };

        public static Analyzer CreateAnalyzerInstance(string name, Type analyzerType)
        {
            try
            {
                // try to get parameterless ctor
                var ctor = analyzerType.GetConstructor(Type.EmptyTypes);
                if (ctor != null)
                    return (Analyzer)ctor.Invoke(null);

                ctor = analyzerType.GetConstructor(ConstructorParameterTypes);

                if (ctor != null)
                    return (Analyzer)ctor.Invoke(ConstructorParameterValues);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Could not create new analyzer instance '{analyzerType.Name}' for field: {name}", e);
            }

            throw new InvalidOperationException($"Could not create new analyzer instance '{analyzerType.Name}' for field: {name}. No recognizable constructor found.");
        }

        public static Type GetAnalyzerType(string name, string analyzerTypeAsString, string databaseName)
        {
            var analyzerType = LuceneAssembly.GetType(analyzerTypeAsString) ??
                               Type.GetType(analyzerTypeAsString) ??
                               Type.GetType("Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers." + analyzerTypeAsString) ??
                               LuceneAssembly.GetType("Lucene.Net.Analysis." + analyzerTypeAsString) ??
                               LuceneAssembly.GetType("Lucene.Net.Analysis.Standard." + analyzerTypeAsString) ??
                               AnalyzerCompilationCache.GetAnalyzerType(analyzerTypeAsString, databaseName);

            if (analyzerType == null)
                throw new InvalidOperationException($"Cannot find analyzer type '{analyzerTypeAsString}' for field: {name}");

            return analyzerType;
        }

        static IndexingExtensions()
        {
            AssemblyLoadContext.Default.Resolving += (context, name) =>
            {
                var assemblyPath = Path.Combine(AppContext.BaseDirectory, name.Name + ".dll");
                if (File.Exists(assemblyPath) == false)
                    return null;

                var loadFromAssemblyPath = context.LoadFromAssemblyPath(assemblyPath);
                if (loadFromAssemblyPath == null)
                    throw new FileNotFoundException("Unable to load " + name.FullName + " from " + assemblyPath);
                return loadFromAssemblyPath;
            };
        }
    }
}

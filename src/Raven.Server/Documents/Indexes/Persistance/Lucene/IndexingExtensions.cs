//-----------------------------------------------------------------------
// <copyright file="IndexingExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Reflection;

using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;

namespace Raven.Server.Documents.Indexes.Persistance.Lucene
{
    public static class IndexingExtensions
    {
        private static readonly Assembly LuceneAssembly = typeof(StandardAnalyzer).GetTypeInfo().Assembly;

        private static readonly Type[] ConstructorParameterTypes = { typeof(global::Lucene.Net.Util.Version) };

        private static readonly object[] ConstructorParameterValues = { global::Lucene.Net.Util.Version.LUCENE_30 };

        public static Analyzer CreateAnalyzerInstance(string name, string analyzerTypeAsString)
        {
            var analyzerType = GetAnalyzerType(name, analyzerTypeAsString);

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
                throw new InvalidOperationException($"Could not create new analyzer instance '{analyzerTypeAsString}' for field: {name}", e);
            }

            throw new InvalidOperationException($"Could not create new analyzer instance '{analyzerTypeAsString}' for field: {name}. No recognizable constructor found.");
        }

        public static Type GetAnalyzerType(string name, string analyzerTypeAsString)
        {
            var analyzerType = LuceneAssembly.GetType(analyzerTypeAsString) ??
                               Type.GetType(analyzerTypeAsString) ??
                               LuceneAssembly.GetType("Lucene.Net.Analysis." + analyzerTypeAsString) ??
                               LuceneAssembly.GetType("Lucene.Net.Analysis.Standard." + analyzerTypeAsString);

            if (analyzerType == null)
                throw new InvalidOperationException($"Cannot find analyzer type '{analyzerTypeAsString}' for field: {name}");

            return analyzerType;
        }
    }
}
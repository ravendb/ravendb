//-----------------------------------------------------------------------
// <copyright file="AbstractCultureCollationAnalyzer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Globalization;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Collation
{
    public class AbstractCultureCollationAnalyzer : CollationAnalyzer
    {
        private static readonly ConcurrentDictionary<Type, CultureInfo> CultureCache = new ConcurrentDictionary<Type, CultureInfo>();

        public AbstractCultureCollationAnalyzer()
        {
            var culture = CultureCache.GetOrAdd(GetType(), t => new CultureInfo(t.Name.Replace(nameof(CollationAnalyzer), string.Empty).ToLowerInvariant()));
            Init(culture);
        }
    }
}

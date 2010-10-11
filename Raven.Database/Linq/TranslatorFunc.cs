using System.Collections.Generic;

namespace Raven.Database.Linq
{
    /// <summary>
    /// 	Defining the translator function for a set of results
    ///     about to sent to the user and apply final processing
    /// </summary>
    public delegate IEnumerable<dynamic> TranslatorFunc(ITranslatorDatabaseAccessor database, IEnumerable<dynamic> source);
}
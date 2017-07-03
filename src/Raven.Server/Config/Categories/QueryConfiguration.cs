using System.ComponentModel;
using Raven.Server.Config.Attributes;

namespace Raven.Server.Config.Categories
{
    public class QueryConfiguration : ConfigurationCategory
    {
        [DefaultValue(null)]//1024 is Lucene.net default, but we don't set it by default
        [ConfigurationEntry("Query.MaxClauseCount", isServerWideOnly: true)]
        [LegacyConfigurationEntry("Raven.MaxClauseCount")]
        public int? MaxClauseCount { get; set; }

        [DefaultValue(true)] //Defaults to use our own implementation of the Lucene query parser
        [ConfigurationEntry("Query.UseLuceneASTParser", isServerWideOnly: true)]
        [LegacyConfigurationEntry("Raven.UseLuceneASTParser")]
        public bool UseLuceneASTParser
        {
            get => Documents.Queries.QueryBuilder.UseLuceneASTParser;

            set => Documents.Queries.QueryBuilder.UseLuceneASTParser = value;
        }
    }
}
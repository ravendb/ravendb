namespace Raven.Server.Documents.Queries.Parser.Lucene
{
    public class PostfixModifiers
    {
        public string Boost { get; set; }
        public string Similarity { get; set; }
        public string Proximity { get; set; }
    }
}


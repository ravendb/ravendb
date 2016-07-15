using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Transformers
{
    public class TransformerBase
    {
        public IndexingFunc TransformResults { get; set; }

        public string Source { get; set; }
    }
}
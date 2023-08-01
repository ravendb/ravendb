namespace Raven.Client.Documents.Indexes
{
    public sealed class CreateFieldOptions
    {
        internal static readonly CreateFieldOptions Default = new CreateFieldOptions();

        public FieldStorage? Storage { get; set; }

        public FieldIndexing? Indexing { get; set; }

        public FieldTermVector? TermVector { get; set; }
    }
}

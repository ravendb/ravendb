namespace Raven.Server.Documents.Queries.Parser.Lucene
{
    public class FieldName
    {
        public FieldName(string field, FieldType type = FieldType.String)
        {
            Field = field;
            Type = type;
        }
        public string Field { get; set; }
        public FieldType Type { get; set; }
        public override string ToString()
        {
            return Field;
        }

        public enum FieldType
        {
            String,
            Long,
            Double
        }
    }
}
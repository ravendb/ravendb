namespace Rhino.DivanDB.Tasks
{
    public class IndexDocumentRangeTask : Task
    {
        public string View { get; set; }
        public string FromKey { get; set; }
        public string ToKey { get; set; }

        public override string ToString()
        {
            return string.Format("IndexDocumentRangeTask - View: {0}, FromKey: {1}, ToKey: {2}", View, FromKey, ToKey);
        }
    }
}
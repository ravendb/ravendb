namespace Rhino.DivanDB.Tasks
{
    public class IndexDocumentTask : Task
    {
        public string Key { get; set; }

        public override string ToString()
        {
            return string.Format("IndexDocumentTask - Key: {0}", Key);
        }
    }
}
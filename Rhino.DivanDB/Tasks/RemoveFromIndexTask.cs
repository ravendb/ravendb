namespace Rhino.DivanDB.Tasks
{
    public class RemoveFromIndexTask : Task
    {
        public string View { get; set; }
        public string[] Keys { get; set; }

        public override string ToString()
        {
            return string.Format("View: {0}, Keys: {1}", View, string.Join(", ", Keys));
        }
    }
}
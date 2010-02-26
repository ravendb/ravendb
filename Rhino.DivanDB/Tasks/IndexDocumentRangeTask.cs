using System;
using Rhino.DivanDB.Indexing;

namespace Rhino.DivanDB.Tasks
{
    public class IndexDocumentRangeTask : Task
    {
        public string View { get; set; }
        public int FromKey { get; set; }
        public int ToKey { get; set; }

        public override string ToString()
        {
            return string.Format("IndexDocumentRangeTask - View: {0}, FromKey: {1}, ToKey: {2}", View, FromKey, ToKey);
        }

        public override void Execute(WorkContext context)
        {
            throw new NotImplementedException();
        }
    }
}
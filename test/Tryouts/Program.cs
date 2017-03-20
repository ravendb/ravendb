using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var testclass = new FastTests.Server.Replication.ReplicationConflictsTests())
            {
                testclass.Conflict_should_be_resolved_for_document_in_different_collections_after_saving_in_new_collection();
            }
        }
    }
}
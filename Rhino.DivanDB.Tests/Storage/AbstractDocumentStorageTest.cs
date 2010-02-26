using System.Diagnostics;
using System.IO;

namespace Rhino.DivanDB.Tests.Storage
{
    public class AbstractDocumentStorageTest
    {
        public AbstractDocumentStorageTest()
        {
            Trace.WriteLine(this.GetType().Name);
            if(Directory.Exists("divan.db.test.esent"))
                Directory.Delete("divan.db.test.esent", true);
        }
    }
}
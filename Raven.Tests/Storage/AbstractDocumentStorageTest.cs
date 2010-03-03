using System.IO;

namespace Raven.Tests.Storage
{
    public class AbstractDocumentStorageTest : WithDebugging
    {
        public AbstractDocumentStorageTest()
        {
            if(Directory.Exists("divan.db.test.esent"))
                Directory.Delete("divan.db.test.esent", true);
        }
    }
}
using System.IO;
using System.IO.IsolatedStorage;

namespace Raven.Studio.Extensions
{
    public static class IsolatedStorageFileExtensions
    {
        public static string ReadEntireFile(this IsolatedStorageFile store, string fileName)
        {
            using (var stream = store.OpenFile(fileName, FileMode.Open, FileAccess.Read))
            using (var reader = new StreamReader(stream))
            {
                return reader.ReadToEnd();
            }
        }

        public static void WriteAllToFile(this IsolatedStorageFile store, string fileName, string contents)
        {
            using (var file = store.OpenFile(fileName, FileMode.OpenOrCreate, FileAccess.Write))
            {
                file.SetLength(0);

                using (var writer = new StreamWriter(file))
                {
                    writer.Write(contents);
                }
            }
        }
    }
}
using System;
using System.IO;
using System.Text;
using Raven.Client.Document;
using SlowTests.Voron;
using Voron;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var store = new DocumentStore { DefaultDatabase = "test", Url = "http://localhost:8080" }.Initialize(true))
            using(var bulk = store.BulkInsert())
            {
                var rand = new Random();
                var numberOfPersons = 50*1000;
                var numberOfBigDocuments = 5*1000;
                var numberOfStoredPeople = 0;
                var numberOfRemainingBigDocuments = numberOfBigDocuments;
                var totalDocuments = numberOfPersons + numberOfRemainingBigDocuments;
                for (var i = 0; i < totalDocuments; i++)
                {
                    if (i + numberOfRemainingBigDocuments >= totalDocuments || rand.NextDouble() <= 0.1)
                    {
                        bulk.Store(new BigDocument(0.1, 2));
                        continue;
                    }
                    bulk.Store(new Person {Name = $"Person{numberOfStoredPeople++}"});
                }
            }
        }
    }

    public class Person
    {
        public string Name { get; set; }
    }

    public class BigDocument
    {
        public BigDocument(double nestedBigDocChance,int maxNesting)
        {
            Field0 = GeneratePageSizeBlob();
            Field1 = GenerateObject(nestedBigDocChance, maxNesting);
            Field2 = GenerateObject(nestedBigDocChance, maxNesting);
            Field3 = GenerateObject(nestedBigDocChance, maxNesting);
        }

        private object GenerateObject(double nestedBigDocChance, int maxNesting)
        {
            var isBigDocument = maxNesting > 0 && rand.NextDouble() <= nestedBigDocChance;
            if(isBigDocument)
                return new BigDocument(nestedBigDocChance,maxNesting-1);
            return GeneratePageSizeBlob();
        }

        private byte[] GeneratePageSizeBlob()
        {
            var buffer = new byte[PageSize];
            rand.NextBytes(buffer);
            return buffer;
        }
        public object Field0 { get; set; }
        public object Field1 { get; set; }
        public object Field2 { get; set; }
        public object Field3 { get; set; }
        private static Random rand = new Random();
        private static readonly int PageSize = 4096;
    }

}

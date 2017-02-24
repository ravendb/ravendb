namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var a = new FastTests.Server.Documents.Indexing.Static.CollisionsOfReduceKeyHashes())
            {
                a.Static_index_should_produce_multiple_outputs(numberOfUsers: 5, locations: new[] {"Israel", "Poland"}).Wait();
            }
        }
    }
}
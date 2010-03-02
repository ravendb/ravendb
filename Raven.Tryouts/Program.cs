namespace Raven.Tryouts
{
    class Program
    {
        const string query = @"
    from doc in docs
    where doc.type <= 1 && doc.user.is_active == false
    select new { Key = doc.title, Value = doc.content, Size = doc.size };
";
        public static void Main()
        {
            
        }
    }
}
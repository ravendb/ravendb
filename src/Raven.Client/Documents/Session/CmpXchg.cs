namespace Raven.Client.Documents.Session
{
    public class CmpXchg : MethodCall
    {
        public static CmpXchg Value(string key)
        {
            return new CmpXchg
            {
                Args = new object[] { key },
            };
        }
    }
}

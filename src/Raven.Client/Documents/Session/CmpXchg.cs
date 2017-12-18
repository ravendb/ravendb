namespace Raven.Client.Documents.Session
{
    public class CmpXchg<T> : MethodCall
    {
        private CmpXchg()
        {
        }

        public static CmpXchg<T> Value(string key)
        {
            return new CmpXchg<T>
            {
                Args = new object[] { key },
            };
        }
    }
}

namespace Raven.Client.Documents.Session
{
    public sealed class CmpXchg : MethodCall
    {
        /// <summary>
        /// Enables the construction of RQL query that specifically retrieve compare exchange values for a given key.
        /// </summary>
        /// <param name="key">The key of the compare exchange.</param>
        public static CmpXchg Value(string key)
        {
            return new CmpXchg
            {
                Args = new object[] { key },
            };
        }
    }
}

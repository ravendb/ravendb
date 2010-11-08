namespace Raven.Client.Silverlight.Common
{
    public class CallbackFunction
    {
        public delegate void Load<in T>(T data);

        public delegate void Store<in T>(T data);

        public delegate void Save(SaveResponse response);
    }
}

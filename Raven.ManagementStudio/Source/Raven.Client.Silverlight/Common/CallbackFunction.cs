namespace Raven.Client.Silverlight.Common
{
    public class CallbackFunction
    {
        public delegate void Load<T>(LoadResponse<T> response);

        public delegate void Store<in T>(T data);

        public delegate void Save<T>(SaveResponse<T> response);
    }
}

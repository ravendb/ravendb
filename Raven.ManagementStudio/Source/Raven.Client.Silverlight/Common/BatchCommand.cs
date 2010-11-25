namespace Raven.Client.Silverlight.Common
{
    using Raven.Client.Silverlight.Data;

    public class BatchCommand<T> where T : JsonDocument
    {
        public RequestMethod Method { get; set; }

        public T Entity { get; set; }
    }
}

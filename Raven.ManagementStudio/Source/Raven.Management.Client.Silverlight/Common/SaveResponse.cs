namespace Raven.Management.Client.Silverlight.Common
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SaveResponse<T> : Response<T>
    {
        /// <summary>
        /// 
        /// </summary>
        public override AsyncAction Action
        {
            get { return AsyncAction.Save; }
        }
    }
}
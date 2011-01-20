namespace Raven.Management.Client.Silverlight.Common
{
    /// <summary>
    /// 
    /// </summary>
    public class DeleteResponse<T> : SaveResponse<T>
    {
        /// <summary>
        /// 
        /// </summary>
        public override AsyncAction Action
        {
            get { return AsyncAction.Delete; }
        }
    }
}
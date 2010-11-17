namespace Raven.Management.Client.Silverlight.Common
{
    using System.Collections.Generic;
    using Database;

    /// <summary>
    /// 
    /// </summary>
    public class CallbackFunction
    {
        #region Delegates

        /// <summary>
        /// 
        /// </summary>
        /// <param name="response"></param>
        public delegate void Batch(IList<BatchResult> response);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="response"></param>
        public delegate void Load<T>(LoadResponse<T> response);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="response"></param>
        public delegate void Save<T>(IList<Response<T>> response);

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity"></param>
        /// <returns></returns>
        public delegate T Track<out T>(JsonDocument entity);

        #endregion
    }
}
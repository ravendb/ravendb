namespace Raven.Management.Client.Silverlight.Common
{
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// 
    /// </summary>
    public static class ResponseExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="responses"></param>
        /// <returns></returns>
        public static IList<SaveResponse<object>> GetSaveResponses(this IList<Response<object>> responses)
        {
            return responses != null
                       ? responses.Where(x => x.Action == AsyncAction.Save).Cast<SaveResponse<object>>().ToList()
                       : null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="responses"></param>
        /// <returns></returns>
        public static IList<DeleteResponse<object>> GetDeleteResponses(this IList<Response<object>> responses)
        {
            return responses != null
                       ? responses.Where(x => x.Action == AsyncAction.Delete).Cast<DeleteResponse<object>>().ToList()
                       : null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="responses"></param>
        /// <returns></returns>
        public static IList<LoadResponse<object>> GetLoadResponses(this IList<Response<object>> responses)
        {
            return responses != null
                       ? responses.Where(x => x.Action == AsyncAction.Load).Cast<LoadResponse<object>>().ToList()
                       : null;
        }
    }
}
namespace Raven.Management.Client.Silverlight.Common.Mappers
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IMapper<out T>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        T Map(string json);
    }
}
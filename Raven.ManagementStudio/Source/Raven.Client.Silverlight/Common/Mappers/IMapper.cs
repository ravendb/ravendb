namespace Raven.Client.Silverlight.Common.Mappers
{
    public interface IMapper<T>
    {
        T Map(string json);
    }
}

namespace Sparrow.Json
{
    public interface IFillFromBlittableJson
    {
        void FillFromBlittableJson(BlittableJsonReaderObject json);
    }
    internal interface IPostJsonDeserialization
    {
        void PostDeserialization();
    }
}

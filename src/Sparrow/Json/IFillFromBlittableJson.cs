namespace Sparrow.Json
{
    public interface IFillFromBlittableJson
    {
        void FillFromBlittableJson(BlittableJsonReaderObject json);
    }
    internal interface IPostDeserialization
    {
        void PostDeserialization();
    }
}

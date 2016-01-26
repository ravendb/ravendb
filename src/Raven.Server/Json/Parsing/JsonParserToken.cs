namespace Raven.Server.Json.Parsing
{
    public enum JsonParserToken
    {
        Null,
        False,
        True,
        String,
        Float,
        Integer,
        Separator,
        StartObject,
        StartArray,
        EndArray,
        EndObject
    }
}
namespace Sparrow.Json.Parsing
{
    public enum JsonParserToken
    {
        None            = 0,
        Null            = 1 << 1,
        False           = 1 << 2,
        True            = 1 << 3,
        String          = 1 << 4,
        Float           = 1 << 5,
        Integer         = 1 << 6,
        Separator       = 1 << 7,
        StartObject     = 1 << 8,
        StartArray      = 1 << 9,
        EndArray        = 1 << 10,
        EndObject       = 1 << 11,
        Blob            = 1 << 12,
    }

    // should never be visible externally
    public enum JsonParserTokenContinuation
    {
        None                    =   0,
        PartialNaN              =   1 << 23,
        PartialPositiveInfinity =   1 << 24,
        PartialNegativeInfinity =   1 << 25,
        PartialNull             =   1 << 26,
        PartialTrue             =   1 << 27,
        PartialString           =   1 << 28,
        PartialNumber           =   1 << 29,
        PartialPreamble         =   1 << 30,
        PartialFalse            =   1 << 31, 
        PleaseRefillBuffer      =   1 << 32,
    }
}

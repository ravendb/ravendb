using System;

namespace Raven.Server.Json.Parsing
{
    public enum JsonParserToken
    {
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
    }

    // should never be visible externally
    public enum JsonParserTokenContinuation
    {
        None                =   0,
        PartialNaN          =   1 << 23,
        PartialNull         =   1 << 24,
        PartialTrue         =   1 << 26,
        PartialString       =   1 << 27,
        PartialNumber       =   1 << 28,
        PartialPreamble     =   1 << 29,
        PartialFalse        =   1 << 30, 
        PleaseRefillBuffer  =   1 << 31,
    }
}
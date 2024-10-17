using System;

namespace Raven.Client.Documents.Indexes.Vector;

public enum EmbeddingType
{
    None = Float32,
    //Floating values
    Float32 = 0,
    //Int8
    Int8 = 1,
    //Binary
    Binary = 2,
    
    Text = 3,
}

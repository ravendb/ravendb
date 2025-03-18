using Raven.Client.Documents.Indexes.Vector;

namespace Raven.Server.Documents.Indexes.VectorSearch;

public struct VectorField(IndexField indexField, VectorOptions vectorOptions)
{
    public readonly IndexField IndexField = indexField;
    public readonly VectorOptions VectorOptions = vectorOptions;
}


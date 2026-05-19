namespace Raven.Server.Documents.ETL;

public enum TaskErrorStep
{
    Unknown = 0,
    Configuration = 1,
    Extraction = 2,
    Transformation = 3,
    Load = 4,
    ModelInference = 5,
    Persistence = 6
}

namespace Raven.Client.Server
{
    public static class Helpers
    {
        public static string ClusterStateMachineValuesPrefix(string databaseName)
        {
            return $"values/{databaseName}/";
        }
    }
}

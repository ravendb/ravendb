namespace Raven.NewClient.Abstractions.Data
{
    public class UserPermission
    {
        public string User { get; set; }
        public DatabaseInfo Database{ get; set; }
        public string Method { get; set; }
        public bool IsGranted { get; set; }
        public string Reason { get; set; }
    }
}

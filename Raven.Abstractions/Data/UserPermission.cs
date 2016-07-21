namespace Raven.Abstractions.Data
{
    public class UserPermission
    {
        /// <summary>
        /// The name of the specific database.
        /// </summary>
        public string User { get; set; }
        /// <summary>
        /// Information about the database.
        /// </summary>
        public DatabaseInfo Database{ get; set; }
        /// <summary>
        /// HTTP Request Method.
        /// </summary>
        public string Method { get; set; }
        /// <summary>
        /// If the premission to do the method is granted
        /// </summary>
        public bool IsGranted { get; set; }
        /// <summary>
        /// The reason for the granted or not granted permission
        /// </summary>
        public string Reason { get; set; }
    }
}

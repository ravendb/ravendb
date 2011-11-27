namespace Rhino.Licensing
{
    /// <summary>
    /// InvalidationType
    /// </summary>
    public enum InvalidationType
    {
        /// <summary>
        /// Can not create a new license
        /// </summary>
        CannotGetNewLicense,

        /// <summary>
        /// License is expired
        /// </summary>
        TimeExpired
    }
}
namespace Raven.Studio.Infrastructure.Validators
{
    /// <summary>
    /// Enables custom Validation attributes to determine when they should suppress validation.
    /// This is necessary because the Silverlight DataGrid sometimes bypasses the INotifyDataErrorInfo interface
    /// and uses Validation attributes on objects to validate them directly. This can lead to undesirable effects,
    /// like showing error messages to the user when they havent even entered values in a new row.
    /// </summary>
    public interface IValidationSuppressible
    {
        bool SuppressValidation { get; }
    }
}
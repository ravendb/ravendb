using System.Net.Mail;

namespace Raven.Server.Commercial.LetsEncrypt;

public class EmailValidator
{
    public static bool IsValid(string email)
    {
        if (string.IsNullOrWhiteSpace(email))
            return false;
        try
        {
            var address = new MailAddress(email);
            return address.Address == email;
        }
        catch
        {
            return false;
        }
    }
}

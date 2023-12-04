using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Microsoft.AspNetCore.Http;
using Raven.Client.Util;
using Raven.Server.Routing;
using Raven.Server.Web.Authentication;

namespace Raven.Server.ServerWide;

public class TwoFactor
{
    private readonly SystemTime _time;
    private readonly ConcurrentDictionary<string, TwoFactorAuthRegistration> _twoFactorAuthTimeByCertThumbprintExpiry = new();

    public TwoFactor(SystemTime time)
    {
        _time = time;
    }

    public TwoFactorAuthRegistration GetAuthRegistration(string thumbprint)
    {
        return _twoFactorAuthTimeByCertThumbprintExpiry.TryGetValue(thumbprint, out var authRegistration) ? authRegistration : null;
    }

    public void RegisterTwoFactorAuthSuccess(TwoFactorAuthRegistration registration)
    {
        registration.Expiry = _time.GetUtcNow() + registration.Period;
        _twoFactorAuthTimeByCertThumbprintExpiry[registration.Thumbprint] = registration;
    }

    public void ForgotTwoFactorAuthSuccess(TwoFactorAuthRegistration registration)
    {
        _twoFactorAuthTimeByCertThumbprintExpiry.TryRemove(new KeyValuePair<string, TwoFactorAuthRegistration>(registration.Thumbprint, registration));
    }
    
    public class TwoFactorAuthRegistration
    {
        public string Thumbprint;
        public DateTime Expiry;
            
        public TimeSpan Period;
        public bool HasLimits;
        public string ExpectedCookieValue;
    }
    
    public bool ValidateTwoFactorConnectionLimits(string certificateThumbprint, string userIpAddress)
    {
        if (_twoFactorAuthTimeByCertThumbprintExpiry.TryGetValue(certificateThumbprint, out var twoFactorAuthRegistration))
        {
            if (_time.GetUtcNow() < twoFactorAuthRegistration.Expiry)
            {
                return true;
            }
            
            // clean up expired two-factor
            ForgotTwoFactorAuthSuccess(twoFactorAuthRegistration);
        }

        return false;
    }
    
    public bool ValidateTwoFactorRequestLimits(RouteInformation routeInformation, HttpContext context, TwoFactor.TwoFactorAuthRegistration twoFactorAuthRegistration, out string msg)
    {
        if (twoFactorAuthRegistration is {HasLimits: true})
        {
            if (routeInformation.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients)
            {
                msg = null;
                return true;
            }

            if (context.Request.Cookies.TryGetValue(TwoFactorAuthentication.CookieName, out var cookieStr) == false)
            {
                msg = $"Missing the '{TwoFactorAuthentication.CookieName}' in the request";
                return false;
            }

            var cookie = MemoryMarshal.Cast<char, byte>(cookieStr);
            var expected = MemoryMarshal.Cast<char, byte>(twoFactorAuthRegistration.ExpectedCookieValue);
            if (CryptographicOperations.FixedTimeEquals(cookie, expected) == false)
            {
                msg = "Expected cookie value does not match provided value";
                return false;
            }
        }
        
        msg = null;
        return true;
    }

}

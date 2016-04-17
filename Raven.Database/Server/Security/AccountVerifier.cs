// -----------------------------------------------------------------------
//  <copyright file="AccountVerifier.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.DirectoryServices.AccountManagement;

namespace Raven.Database.Server.Security
{
    public static class AccountVerifier
    {
        public static bool UserExists(string domainAndUserName)
        {
            var splitDomain = SplitDomain(domainAndUserName);
            string domain = splitDomain[0];
            string userName = splitDomain[1];

            try
            {
                using (var pc = GetPrincipalContext(domain))
                using (var p = Principal.FindByIdentity(pc, IdentityType.SamAccountName, userName))
                {
                    return p != null;
                }
            }
            catch (PrincipalServerDownException)
            {
                return false;
            }
        }

        public static bool GroupExists(string domainAndGroupName)
        {
            var splitDomain = SplitDomain(domainAndGroupName);
            string domain = splitDomain[0];
            string groupName = splitDomain[1];

            try
            {
                using (var context = GetPrincipalContext(domain))
                {
                    var groupPrincipal = GroupPrincipal.FindByIdentity(context, groupName);
                    return groupPrincipal != null;
                }
            }
            catch (PrincipalServerDownException)
            {
                return false;
            }
          
        }

        /// <summary>
        /// We return different principal context based on supplied domain. 
        /// 
        /// If machine is in workgroup then domain is supplied as machine name (which we detect). 
        /// 
        /// new PrincipalContext(ContextType.Domain, Environment.MachineName) doesn't work, since 
        ///    LDAP is not available. 
        /// </summary>
        /// <param name="domain"></param>
        /// <returns></returns>
        private static PrincipalContext GetPrincipalContext(string domain)
        {
            if (Environment.MachineName.Equals(domain, StringComparison.OrdinalIgnoreCase))
            {
                return new PrincipalContext(ContextType.Machine);
            } 
            return new PrincipalContext(ContextType.Domain, domain);
        }

        private static string[] SplitDomain(string input)
        {
            var tokens = input.Split(new [] { '\\' }, 2);
            if (tokens.Length != 2)
            {
                throw new ArgumentException(nameof(input));
            }

            return tokens;
        }
    }
}
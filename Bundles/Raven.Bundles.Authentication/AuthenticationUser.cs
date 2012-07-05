using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Raven.Bundles.Authentication
{
	public class AuthenticationUser
	{
		public string Name { get; set; }
		public string Id { get; set; }
		public bool Admin { get; set; }
		public string[] AllowedDatabases { get; set; }

		public UserDatabaseAccess[] Databases { get; set; }

		protected string HashedPassword { get; private set; }

		private Guid passwordSalt;

		protected Guid PasswordSalt
		{
			get
			{
				if (passwordSalt == Guid.Empty)
					passwordSalt = Guid.NewGuid();
				return passwordSalt;
			}
			set { passwordSalt = value; }
		}


		public AuthenticationUser SetPassword(string pwd)
		{
			HashedPassword = GetHashedPassword(pwd);
			return this;
		}

		private string GetHashedPassword(string pwd)
		{
			using (var sha = SHA256.Create())
			{
				var bytes = PasswordSalt.ToByteArray().Concat(Encoding.Unicode.GetBytes(pwd)).ToArray();

				return Convert.ToBase64String(sha.ComputeHash(bytes));
			}
		}

		public bool ValidatePassword(string maybePwd)
		{
			return HashedPassword == GetHashedPassword(maybePwd);
		}

	}
}
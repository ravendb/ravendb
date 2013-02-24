using System.IO;
using System.Net;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			var listener = new HttpListener
			{
				Prefixes = { "http://+:8081/" },
				AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication | AuthenticationSchemes.Anonymous,
				AuthenticationSchemeSelectorDelegate = request =>
				{
					switch (request.QueryString["auth"])
					{
						case "anon":
							return AuthenticationSchemes.Anonymous;
						case "win":
							return AuthenticationSchemes.IntegratedWindowsAuthentication;
						case "both":
							return AuthenticationSchemes.IntegratedWindowsAuthentication | AuthenticationSchemes.Anonymous;
						default:
							return AuthenticationSchemes.None;
					} 
				}
			};
			listener.Start();

			while (true)
			{
				var ctx = listener.GetContext();
				using (var writer = new StreamWriter(ctx.Response.OutputStream))
				{
					if (ctx.User == null)
					{
						writer.WriteLine("No user");
						continue;
					}

					writer.WriteLine(ctx.User.Identity.Name);
				}
			}
		}
	}
}

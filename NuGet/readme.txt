=======================================================================================================
 _____ __  __ _____   ____  _____ _______       _   _ _______   _   _  ____ _______ _____ _____ ______ 
|_   _|  \/  |  __ \ / __ \|  __ \__   __|/\   | \ | |__   __| | \ | |/ __ \__   __|_   _/ ____|  ____|
  | | | \  / | |__) | |  | | |__) | | |  /  \  |  \| |  | |    |  \| | |  | | | |    | || |    | |__   
  | | | |\/| |  ___/| |  | |  _  /  | | / /\ \ | . ` |  | |    | . ` | |  | | | |    | || |    |  __|  
 _| |_| |  | | |    | |__| | | \ \  | |/ ____ \| |\  |  | |    | |\  | |__| | | |   _| || |____| |____ 
|_____|_|  |_|_|     \____/|_|  \_\ |_/_/    \_\_| \_|  |_|    |_| \_|\____/  |_|  |_____\_____|______|
                                                                                                       
=======================================================================================================

If your application uses one of the following NuGet packages

- Microsoft.AspNet.WebApi.*
- Microsoft.Owin.*
- Newtonsoft.Json

then, if 'EmbeddableDocumentStore' store is used, packages must be in following versions

- Microsoft.AspNet.WebApi.* - 5.2.2 (or higher)
- Microsoft.Owin.* - 3.0.0 (or higher)
- Newtonsoft.Json - 6.0.6 (or higher)

with proper assembly binding redirect in application configuration file
http://msdn.microsoft.com/en-us/library/7wd6ex19%28v=vs.110%29.aspx

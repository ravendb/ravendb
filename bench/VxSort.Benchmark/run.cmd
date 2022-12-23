FOR %%G IN (telegram,chrome,typora,firefox,rider64,devenv,msbuild,spotify) DO (pssuspend %%G 1> nul 2> nul)

dotnet run -c release -- %1 %2 %3 %4 %5 %6 %7 %8 %9

FOR %%G IN (telegram,chrome,typora,firefox,rider64,devenv,msbuild,spotify) DO (pssuspend -r %%G 1> nul 2> nul)
FROM mcr.microsoft.com/dotnet/core/sdk:5.0 AS build
WORKDIR /app

RUN curl -sL https://deb.nodesource.com/setup_10.x | bash -
RUN apt-get install -y gcc g++ make nodejs && node --version

COPY *.sln NuGet.Config *.ruleset *.snk *.txt ./
COPY libs/ ./libs

COPY src/ ./src

RUN ls && du -sh ./src/* && du -sh ./src/Raven.Server/*

RUN dotnet restore ./src/Raven.Server/Raven.Server.csproj
RUN dotnet build ./src/Raven.Server/Raven.Server.csproj && \
    echo '{}' > ./src/Raven.Server/bin/Debug/netcoreapp3.1/settings.json

COPY tools/ ./tools
RUN dotnet build ./tools/TypingsGenerator/TypingsGenerator.csproj \
    && cd src/Raven.Studio \
    && npm install && npm run gulp restore && npm run gulp compile

ENTRYPOINT [ "dotnet", "./src/Raven.Server/bin/Debug/net5.0/Raven.Server.dll" ]

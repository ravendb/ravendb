# escape=`

FROM mcr.microsoft.com/powershell:lts-nanoserver-1809

ENV RAVEN_ARGS='' RAVEN_SETTINGS='' RAVEN_Setup_Mode='Initial' RAVEN_DataDir='RavenData' RAVEN_ServerUrl_Tcp='38888' RAVEN_AUTO_INSTALL_CA='true' RAVEN_IN_DOCKER='true'

EXPOSE 8080 38888 161

VOLUME C:/RavenDB/Server/RavenData C:/RavenDB/Config

WORKDIR C:/RavenDB

COPY RavenDB.zip run-raven.ps1 ./

USER ContainerAdministrator

RUN tar -xf ravendb.zip `
    && del ravendb.zip `
    && icacls C:\RavenDB /grant "User Manager\ContainerUser:(OI)(CI)M" /T /Q `
    && icacls C:\RavenDB /setowner "User Manager\ContainerUser" /T /Q `
    # RavenDB-14874 workaround
    && mkdir C:\ProgramData\Microsoft\Crypto\RSA\MachineKeys ` 
    && icacls C:\ProgramData\Microsoft\Crypto\RSA\MachineKeys /grant "User Manager\ContainerUser":(OI)(CI)M  

USER ContainerUser

WORKDIR C:/RavenDB/Server 

ADD settings.json https://ravendb-docker.s3.amazonaws.com/vcruntime140.dll .\

CMD [ "pwsh.exe", "-File", "C:\\RavenDB\\run-raven.ps1" ]

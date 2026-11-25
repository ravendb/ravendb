import { useEffect, useMemo, useRef, useState } from "react";
import { Control, useWatch } from "react-hook-form";
import { debounce } from "lodash";
import { useServices } from "components/hooks/useServices";
import recentError = require("common/notifications/models/recentError");
import generalUtils = require("common/generalUtils");
import { RemoteServerBuildInfoArgs } from "commands/database/studio/getRemoteServerVersionWithDatabasesCommand";
import { MigrateRavenDbFormData, serverUrlSchema } from "./migrateRavenDbValidation";
import { SourceServerVersion } from "./migrateVersionSupport";

type BuildInfoWithResourceNames = Raven.Server.Smuggler.Migration.BuildInfoWithResourceNames;

export interface RemoteServerInfo {
    version: SourceServerVersion | null;
    databaseNames: string[];
    fileSystemNames: string[];
    isAuthorized: boolean;
    hasUnsecuredBasicAuthOption: boolean;
    error: string | null;
    isLoading: boolean;
}

interface DetectionResult {
    url: string;
    info: BuildInfoWithResourceNames | null;
    error: string | null;
}

const DETECTION_DELAY_MS = 700;

export function useRemoteServerInfo(control: Control<MigrateRavenDbFormData>, canDetect: boolean): RemoteServerInfo {
    const { tasksService } = useServices();
    const [
        serverUrl,
        isSkipServerCertificateValidation,
        authenticationMethod,
        userName,
        password,
        domain,
        apiKey,
        isEnableBasicAuthenticationOverUnsecuredHttp,
    ] = useWatch({
        control,
        name: [
            "server.serverUrl",
            "server.isSkipServerCertificateValidation",
            "server.authenticationMethod",
            "server.userName",
            "server.password",
            "server.domain",
            "server.apiKey",
            "server.isEnableBasicAuthenticationOverUnsecuredHttp",
        ],
    });

    const [result, setResult] = useState<DetectionResult | null>(null);
    const latestRequestId = useRef(0);

    const isWindows = authenticationMethod === "windows";
    const isApiKey = authenticationMethod === "apiKey";
    const isHttps = serverUrl.toLowerCase().startsWith("https://");

    const requestArgs = useMemo<RemoteServerBuildInfoArgs>(
        () => ({
            serverUrl,
            userName: isWindows ? userName : "",
            password: isWindows ? password : "",
            domain: isWindows ? domain : "",
            apiKey: isApiKey ? apiKey : "",
            enableBasicAuthenticationOverUnsecuredHttp: isApiKey && isEnableBasicAuthenticationOverUnsecuredHttp,
            skipServerCertificateValidation: isHttps && isSkipServerCertificateValidation,
        }),
        [
            serverUrl,
            isWindows,
            isApiKey,
            isHttps,
            userName,
            password,
            domain,
            apiKey,
            isEnableBasicAuthenticationOverUnsecuredHttp,
            isSkipServerCertificateValidation,
        ]
    );

    const isDetectable = canDetect && serverUrlSchema.isValidSync(serverUrl);

    const detect = useMemo(
        () =>
            debounce(async (args: RemoteServerBuildInfoArgs) => {
                const requestId = ++latestRequestId.current;
                let next: DetectionResult;
                try {
                    const info = await tasksService.getRemoteServerBuildInfo(args);
                    next = { url: args.serverUrl, info, error: null };
                } catch (e) {
                    const { message } = recentError.tryExtractMessageAndException(e.responseText);
                    next = { url: args.serverUrl, info: null, error: generalUtils.trimMessage(message, 1024) };
                }
                if (requestId === latestRequestId.current) {
                    setResult(next);
                }
            }, DETECTION_DELAY_MS),
        [tasksService]
    );

    // the remote server is queried only once the user stops typing its url or credentials
    useEffect(() => {
        if (!isDetectable) {
            return;
        }
        detect(requestArgs);
        return () => detect.cancel();
    }, [requestArgs, isDetectable, detect]);

    const current = isDetectable && result?.url === serverUrl ? result : null;
    const info = current?.info && current.info.MajorVersion !== "Unknown" ? current.info : null;

    const version = useMemo<SourceServerVersion | null>(
        () =>
            info && {
                majorVersion: info.MajorVersion,
                buildVersion: info.BuildVersion,
                productVersion: info.ProductVersion,
                fullVersion: info.FullVersion,
            },
        [info]
    );

    return {
        version,
        databaseNames: info?.DatabaseNames ?? [],
        fileSystemNames: info?.FileSystemNames ?? [],
        isAuthorized: info?.Authorized ?? true,
        hasUnsecuredBasicAuthOption: info?.IsLegacyOAuthToken ?? false,
        error: current?.error ?? null,
        isLoading: isDetectable && !current,
    };
}

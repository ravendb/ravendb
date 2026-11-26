import { useEffect, useMemo, useRef, useState } from "react";
import { Control, useWatch } from "react-hook-form";
import { debounce } from "lodash";
import { useServices } from "components/hooks/useServices";
import recentError = require("common/notifications/models/recentError");
import generalUtils = require("common/generalUtils");
import getRemoteServerVersionWithDatabasesCommand = require("commands/database/studio/getRemoteServerVersionWithDatabasesCommand");
import { MigrateRavenDbFormData, serverUrlSchema } from "./migrateRavenDbValidation";
import { SourceServerVersion, getEffectiveAuthenticationMethod } from "./migrateVersionSupport";

type BuildInfoWithResourceNames = Raven.Server.Smuggler.Migration.BuildInfoWithResourceNames;
type RemoteServerBuildInfoArgs = ConstructorParameters<typeof getRemoteServerVersionWithDatabasesCommand>[0];

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

    const isDetectable = canDetect && serverUrlSchema.isValidSync(serverUrl);
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

    // credentials go only to a source already detected as legacy, the first detection works without them
    const effectiveAuthenticationMethod = getEffectiveAuthenticationMethod(authenticationMethod, version);
    const isWindows = effectiveAuthenticationMethod === "windows";
    const isApiKey = effectiveAuthenticationMethod === "apiKey";
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

    const detect = useMemo(
        () =>
            debounce(async (args: RemoteServerBuildInfoArgs) => {
                const requestId = ++latestRequestId.current;
                const url = args.serverUrl;
                const isLatest = () => requestId === latestRequestId.current;
                try {
                    const info = await tasksService.getRemoteServerBuildInfo(args);
                    if (isLatest()) {
                        setResult({ url, info, error: null });
                    }
                } catch (e) {
                    if (isLatest()) {
                        const { message } = recentError.tryExtractMessageAndException(e.responseText);
                        const error = generalUtils.trimMessage(message, 1024);
                        // a failed credential refetch keeps the detected version, so the credential inputs stay
                        setResult((prev) => ({ url, info: prev?.url === url ? prev.info : null, error }));
                    }
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

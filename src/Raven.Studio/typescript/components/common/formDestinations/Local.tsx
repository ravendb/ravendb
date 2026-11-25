import Spinner from "react-bootstrap/Spinner";
import { useFormContext, useWatch } from "react-hook-form";
import { FormGroup, FormLabel, FormPathSelector } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { useServices } from "components/hooks/useServices";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { FormDestinations } from "./utils/formDestinationsTypes";
import DestinationCard from "./DestinationCard";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function Local() {
    return (
        <DestinationCard type="local">
            <LocalFields />
        </DestinationCard>
    );
}

interface LocalFieldsProps {
    rootPath?: string;
    label?: string;
    selectorTitle?: string;
}

export function LocalFields({ rootPath, label, selectorTitle }: LocalFieldsProps) {
    const { control } = useFormContext<FormDestinations>();
    const t = useStudioTranslation("formDestinations");
    const folderPath = useWatch({ control, name: "destinations.local.folderPath" });

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { tasksService } = useServices();

    const getLocalFolderPathsProvider = (path: string) => async () => {
        const dto = await tasksService.getLocalFolderPathOptions(path, databaseName);
        return dto?.List ?? [];
    };

    const asyncGetBackupLocation = useAsyncDebounce(async () => {
        if (!folderPath) {
            return null;
        }
        return tasksService.getBackupLocation(folderPath, databaseName);
    }, [folderPath]);

    return (
        <FormGroup marginClass="m-0">
            <FormLabel>{label ?? t("local.folderPath")}</FormLabel>
            <FormPathSelector
                control={control}
                name="destinations.local.folderPath"
                selectorTitle={selectorTitle ?? t("local.selectFolderPath")}
                placeholder={
                    rootPath ? t("local.pathPlaceholder", { context: "relative" }) : t("local.pathPlaceholder")
                }
                getPathsProvider={getLocalFolderPathsProvider}
                getPathDependencies={(path: string) => [path, databaseName]}
            />
            {rootPath && (
                <div className="small text-muted mt-1">
                    <StudioTrans
                        ns="formDestinations"
                        i18nKey="local.rootPathInfo"
                        components={{ code: <code /> }}
                        options={{ rootPath }}
                    />
                </div>
            )}
            {folderPath && asyncGetBackupLocation.loading && (
                <RichAlert variant="info" className="mt-2">
                    <Spinner size="sm" />
                </RichAlert>
            )}
            {folderPath && asyncGetBackupLocation.result?.List?.length > 0 && (
                <RichAlert variant="info" className="mt-2">
                    {asyncGetBackupLocation.result.List.map((location) => (
                        <div key={location.NodeTag + location.FullPath} className="small text-break">
                            <StudioTrans
                                ns="formDestinations"
                                i18nKey="local.nodeTag"
                                components={{ strong: <strong /> }}
                                options={{ nodeTag: location.NodeTag }}
                            />
                            <br />
                            <StudioTrans
                                ns="formDestinations"
                                i18nKey="local.path"
                                components={{ strong: <strong /> }}
                                options={{ path: location.FullPath }}
                            />
                            <br />
                            {location.Error ? (
                                <strong>{location.Error}</strong>
                            ) : location.FreeSpaceHumane ? (
                                <span>
                                    <StudioTrans
                                        ns="formDestinations"
                                        i18nKey="local.freeSpace"
                                        components={{ strong: <strong /> }}
                                        options={{ freeSpace: location.FreeSpaceHumane }}
                                    />
                                    {location.TotalSpaceHumane && (
                                        <span>
                                            {" "}
                                            <StudioTrans
                                                ns="formDestinations"
                                                i18nKey="local.totalSpace"
                                                components={{ strong: <strong /> }}
                                                options={{ totalSpace: location.TotalSpaceHumane }}
                                            />
                                        </span>
                                    )}
                                </span>
                            ) : (
                                <strong>{t("local.unreachable")}</strong>
                            )}
                        </div>
                    ))}
                </RichAlert>
            )}
        </FormGroup>
    );
}

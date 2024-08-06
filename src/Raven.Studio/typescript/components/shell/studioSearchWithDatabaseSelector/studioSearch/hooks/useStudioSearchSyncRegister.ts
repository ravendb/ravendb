import databasesManager from "common/shell/databasesManager";
import intermediateMenuItem from "common/shell/menu/intermediateMenuItem";
import leafMenuItem from "common/shell/menu/leafMenuItem";
import {
    StudioSearchMenuItemType,
    StudioSearchItem,
    StudioSearchItemType,
    StudioSearchItemEvent,
} from "../studioSearchTypes";
import { exhaustiveStringTuple } from "components/utils/common";
import { useEffect, useCallback } from "react";
import IconName from "typings/server/icons";
import { useAppUrls } from "components/hooks/useAppUrls";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";

interface UseStudioSearchSyncRegisterParams {
    register: (type: StudioSearchItemType, newItems: StudioSearchItem[]) => void;
    menuItems: menuItem[];
    goToUrl: (url: string, newTab: boolean) => void;
    resetDropdown: () => void;
}

export function useStudioSearchSyncRegister(props: UseStudioSearchSyncRegisterParams) {
    const { register, menuItems, goToUrl, resetDropdown } = props;

    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const allDatabases = useAppSelector(databaseSelectors.allDatabases);
    const collectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames);
    const getCanHandleOperation = useAppSelector(accessManagerSelectors.getCanHandleOperation);

    const { appUrl } = useAppUrls();

    const goToMenuItem = useCallback(
        (item: leafMenuItem, event: StudioSearchItemEvent) => {
            const url = item.dynamicHash();
            goToUrl(url, event.ctrlKey);
        },
        [goToUrl]
    );

    const goToCollection = useCallback(
        (collectionName: string, event: StudioSearchItemEvent) => {
            const url = appUrl.forDocuments(collectionName, activeDatabaseName);
            goToUrl(url, event.ctrlKey);
        },
        [activeDatabaseName, appUrl, goToUrl]
    );

    const handleDatabaseSwitch = useCallback(
        (databaseName: string, e: StudioSearchItemEvent) => {
            resetDropdown();

            if (e.ctrlKey) {
                window.open(appUrl.forDocumentsByDatabaseName(null, databaseName));
            }
            const db = databasesManager.default.getDatabaseByName(databaseName);
            databasesManager.default.activate(db);
        },
        [resetDropdown, appUrl]
    );

    // Register collections
    useEffect(() => {
        register(
            "collection",
            collectionNames.map((name) => ({
                type: "collection",
                icon: "documents",
                onSelected: (e) => goToCollection(name, e),
                text: name,
            }))
        );
    }, [collectionNames, goToCollection, register]);

    // Register databases to switch (show only enabled and not currently active)
    useEffect(() => {
        register(
            "database",
            allDatabases
                .filter((x) => x.name !== activeDatabaseName && !x.isDisabled)
                .map((db) => ({
                    type: "database",
                    icon: "database",
                    onSelected: (e) => handleDatabaseSwitch(db.name, e),
                    text: db.name,
                    alternativeTexts: ["db", "Database", "Active Database", "Select Database"],
                }))
        );
    }, [activeDatabaseName, allDatabases, appUrl, register, handleDatabaseSwitch]);

    const getMenuItemType = useCallback((route: string): StudioSearchMenuItemType => {
        if (route === "virtual") {
            return null;
        }

        const isDatabaseRoute = route.startsWith(databaseRoutePrefix);

        if (isDatabaseRoute) {
            const databaseRoute = route.replace(databaseRoutePrefix, "");

            for (const [prefix, databaseMenuItemType] of Object.entries(databaseRouteMappings)) {
                if (databaseRoute.startsWith(prefix)) {
                    return databaseMenuItemType;
                }
            }
        }

        return "serverMenuItem";
    }, []);

    // Register menu items
    useEffect(() => {
        const searchItems: StudioSearchItem[] = [];
        const menuLeafs: leafMenuItem[] = [];

        const crawlMenu = (item: menuItem) => {
            if (item instanceof leafMenuItem) {
                menuLeafs.push(item);
            } else if (item instanceof intermediateMenuItem) {
                item.children.forEach(crawlMenu);
            }
        };

        menuItems.forEach(crawlMenu);

        menuLeafs
            .filter((item) => item.search?.isExcluded !== true)
            .forEach((item) => {
                const canHandle = item.requiredAccess ? getCanHandleOperation(item.requiredAccess) : true;
                if (!canHandle) {
                    return;
                }

                const route = Array.isArray(item.route) ? item.route.find((x) => x) : item.route;

                const type = getMenuItemType(route);
                if (type === null) {
                    return;
                }

                const innerActions = (item.search?.innerActions ?? []).map((x) => ({
                    text: x.name,
                    alternativeTexts: x.alternativeNames,
                }));

                searchItems.push({
                    type,
                    text: item.search?.overrideTitle ?? item.title,
                    route,
                    alternativeTexts: item.search?.alternativeTitles ?? [],
                    icon: item.css.replace("icon-", "") as IconName,
                    onSelected: (e) => goToMenuItem(item, e),
                    innerActions,
                });
            });

        allMenuItemTypes.forEach((type) => {
            register(
                type,
                searchItems.filter((x) => x.type === type)
            );
        });
    }, [menuItems, goToMenuItem, getMenuItemType, getCanHandleOperation, register]);
}

const databaseRoutePrefix = "databases/";

const databaseRouteMappings: Record<string, StudioSearchMenuItemType> = {
    tasks: "tasksMenuItem",
    indexes: "indexesMenuItem",
    query: "indexesMenuItem",
    documents: "documentsMenuItem",
    edit: "documentsMenuItem",
    patch: "documentsMenuItem",
    identities: "documentsMenuItem",
    cmpXchg: "documentsMenuItem",
    settings: "settingsMenuItem",
    manageDatabaseGroup: "settingsMenuItem",
    advanced: "settingsMenuItem",
    status: "statsMenuItem",
};

const allMenuItemTypes = exhaustiveStringTuple<StudioSearchMenuItemType>()(
    "serverMenuItem",
    "documentsMenuItem",
    "indexesMenuItem",
    "tasksMenuItem",
    "settingsMenuItem",
    "statsMenuItem"
);
